use clob_engine::Tracing;
use clob_engine::order_book::types::{CancelOutcome, OrderType};
use clob_engine::{MatchingEngine, NewOrder};

use orderbook_proto::{ CancelOrderRequest, CancelOrderResponse, ModifyOrderRequest, ModifyOrderResponse,
    NewOrderRequest, NewOrderResponse, BookRequest, BookResponse,BookDepth, OrderBook, OrderBookServer};
use tracing::field::Empty;
use tracing_subscriber::fmt::format::FmtSpan;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, transport::Server};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

fn init_tracing() {
    let subscriber = fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_span_events(FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set tracing subscriber");
}

#[derive(Debug)]
pub struct TradingServer {
    engine: Arc<Mutex<MatchingEngine>>,
    security_registry : Arc<Mutex<HashMap<String, Uuid>>>,
    order_registry : Arc<Mutex<HashMap<Uuid, usize>>>
}
impl TradingServer {
    fn new() -> Self {
        Self {
            engine: Arc::new(Mutex::new(MatchingEngine::new())),
            security_registry : Arc::new(Mutex::new(HashMap::new())),
            order_registry : Arc::new(Mutex::new(HashMap::new()))
        }
    }
    fn get_security_id(&self, security_name : String) -> Result<Uuid, Status>{
        match self.security_registry.lock(){
            Ok(mut gaurd) => { // here we take the gaurd as mutable, not self as mutable. after that we can perform changes.
                let id =  gaurd.entry(security_name).or_insert_with(|| Uuid::new_v4());
                Ok(*id)
            },
            Err(_) => {
                Err(Status::internal("unable to acquire lock"))
            }
        }
    }
}

#[tonic::async_trait]
impl OrderBook for TradingServer {
    async fn new_order(
        &self,
        request: Request<NewOrderRequest>,
    ) -> Result<Response<NewOrderResponse>, Status> {
        let order_id = Uuid::new_v4();

        let order_data = request.into_inner();
        
        let security_id = self.get_security_id(order_data.security_name)?;
        let match_span = Tracing::match_order_span(
            order_id.to_string(),
            Empty,
            Empty,
            if order_data.is_buy_side {
                "buy"
            } else {
                "sell"
            },
            order_data.is_buy_side,
            Empty,
            Empty,
            Empty
        );

        match self.order_registry.lock(){
            Ok(mut registry_gaurd) => {

                match self.engine.lock(){
                    Ok(mut engine_lock) => {
        
                        let match_response = engine_lock.match_order(
                            NewOrder {
                                engine_order_id: order_id,
                                price: order_data.price,
                                initial_quantity: order_data.quantity,
                                current_quantity: order_data.quantity,
                                is_buy_side: order_data.is_buy_side,
                                security_id,
                                order_type: if order_data.order_type == 0 { OrderType::Market(if order_data.price.is_some() { order_data.price} else {None})} else {OrderType::Limit},
                            },
                            &match_span,
                        );
                        match match_response{
                            Ok(res) => {
                                match res {
                                    Some(index) => {
                                        registry_gaurd.insert(order_id, index);
                                        Ok(Response::new(NewOrderResponse{
                                              order_id : order_id.to_string(),
                                              status : 200,
                                              order_index : Some(index as u32),
                                              cause : None
                                        }))
                                      },
                                      None => {
                                        Ok(Response::new(NewOrderResponse {
                                              order_id: order_id.to_string(),
                                              status: 200,
                                              order_index : None,
                                              cause : None
                                        }))
                                    }
                                }
                            },
                            Err(e) => {
                                Err(Status::internal(format!("error occured in matching due to {}",e)))
                            }
                        }
                    }
                    Err(e) => {
                        Err(Status::internal(format!("new order engine mutex poisened due to {}",e)))
                    }
                }
            }
            Err(e) => {
                Err(Status::internal(format!("order registry mutex poisened due to {}",e)))
            }
        }
        
        

    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let cancel_data = request.into_inner();
        let order_id = Uuid::parse_str(&cancel_data.order_id).map_err(|_| Status::internal("failed to convert str to UUID"))?;
        let cancel_span = Tracing::cancel_span(order_id, 
           false,
           ""
        );
        match self.order_registry.lock(){
            Ok(order_gaurd) => {
                if let Some(_) = order_gaurd.get_key_value(&order_id){
                    match self.engine.lock(){
                        Ok(mut engine_gaurd) => {
        
                            let cancel_response = engine_gaurd.cancel(order_id, &cancel_span);
                            match cancel_response {
                                Ok(outcome) => {
                                    match outcome{
                                        CancelOutcome::Success => {
                                            Ok(Response::new(CancelOrderResponse {
                                                order_id : order_id.to_string(),
                                                status: 200,
                                                cause: Some("cancellation succesfull".to_owned()),
                                            }))
                                        }
                                        CancelOutcome::Failed => {
                                            Ok(Response::new(CancelOrderResponse {
                                                order_id : order_id.to_string(),
                                                status: 400,
                                                cause: Some("cancellation failed".to_owned()),
                                            }))
                                        }
                                    }
                                }
                                Err(e) => {
                                    Err(Status::internal(format!("error occured due to {}",e)))
                                }
                            }
                        }
                        Err(e) => {
                            Err(Status::internal(format!("cancel engine mutex poisened due to {}",e)))
                        }
                    }
                }
                else {
                    Err(Status::internal("order doesn't exist in server order registry"))
                }

            }
            Err(e) => {
                Err(Status::internal(format!("{}",e)))
            }
        }
    }
    async fn modify_order(
        &self,
        request: Request<ModifyOrderRequest>,
    ) -> Result<Response<ModifyOrderResponse>, Status> {
        let modify_data = request.into_inner();

        let order_id = Uuid::parse_str(&modify_data.order_id).map_err(|_| Status::internal("failed to convert str to UUID"))?;
        let modify_span = Tracing::modify_span(order_id.to_string(), 
            false, 
            Empty, 
            Empty, 
            Empty, 
            if modify_data.side {
                "buy"
            }else {
                "sell"
            }, 
            modify_data.side,
            0, 
            0
        );
        match self.order_registry.lock(){
            Ok(order_gaurd) => {

                if let Some(_) = order_gaurd.get_key_value(&order_id){
                    match self.engine.lock(){
                        Ok(mut engine_gaurd) => {
                            let modify_result = engine_gaurd.modify(order_id, modify_data.new_price, modify_data.new_quantity, &modify_span);
                            match modify_result {
                                Ok(outcome) => {
                                    Ok(Response::new(ModifyOrderResponse {
                                        order_id : order_id.to_string(),
                                        status: 200,
                                        output : Some(outcome.to_string())
                                }))
                                },
                                Err(e) => {
                                    Err(Status::internal(format!("{}",e)))
                                }
                            }
        
                        },
                        Err(e) => {
                            Err(Status::internal(format!("modify engine mutex poisened due to {}",e)))
                        }
                    }
                }
                else {
                    Err(Status::internal("order doesn't exist in server order registry"))
                }
            }
            Err(e) => {
                Err(Status::internal(format!("{}",e)))
            }
        }

    }
    async fn book_depth(&self, request: Request<BookRequest>) -> Result<Response<BookResponse>, Status>{
        
        let match_span = Tracing::depth_span(
            Empty,
            Empty,
            Empty
        );
        let req = request.into_inner();
        let security_id = self.get_security_id(req.security_name)?;

        let engine = self
            .engine
            .lock()
            .map_err(|_| Status::internal("engine mutex poisoned"))?;

        match engine.depth(security_id, req.level_count, &match_span){
            Ok(book_depth) => {
                let proto_depth = BookDepth::from(book_depth); // can directly use book_depth.into() also
                Ok(Response::new(BookResponse {
                    status : 200,
                    book_depth : Some(proto_depth)
                }))
            }
            Err(_) => {
                Ok(Response::new(BookResponse {
                    status : 200,
                    book_depth : None
                }))
            }
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing();
    let address = "[::1]:50051".parse()?;
    let trading_server = TradingServer::new();

    let _ = Server::builder()
        .add_service(OrderBookServer::new(trading_server)) //orderbook server is a wrapper, which 'listen for incoming grpc calls', 'deserialize protobuf message', 'call trading server methods','serialize response back'.
        .serve(address)
        .await;

    Ok(())
}