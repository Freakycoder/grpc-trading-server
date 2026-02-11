use clob_engine::Tracing;
use clob_engine::order_book::types::OrderType;
use clob_engine::{MatchingEngine, NewOrder};
use orders::order_book_server::{OrderBook, OrderBookServer};
use orders::{
    CancelOrderRequest, CancelOrderResponse, ModifyOrderRequest, ModifyOrderResponse,
    NewOrderRequest, NewOrderResponse, BookRequest, BookResponse,BookDepth,PriceLevelDepth
};
use tracing::field::Empty;
use tracing_subscriber::fmt::format::FmtSpan;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, transport::Server};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

pub mod client;
pub mod orders {
    tonic::include_proto!("orders");
}

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
    security_id : Uuid
}

#[tonic::async_trait]
impl OrderBook for TradingServer {
    async fn new_order(
        &self,
        request: Request<NewOrderRequest>,
    ) -> Result<Response<NewOrderResponse>, Status> {
        let order_id = Uuid::new_v4();

        let order_data = request.into_inner();

        let match_span = Tracing::match_order_span(
            order_id.to_string(),
            Empty,
            Empty,
            if order_data.is_buy_side {
                "buy"
            } else {
                "sell"
            },
            true,
            Empty,
            Empty,
            Empty
        );

        let mut engine = self
            .engine
            .lock()
            .map_err(|_| Status::internal("engine mutex poisoned"))?;

        let _ = engine.match_order(
            NewOrder {
                engine_order_id: order_id,
                price: order_data.price,
                initial_quantity: order_data.quantity,
                current_quantity: order_data.quantity,
                is_buy_side: order_data.is_buy_side,
                security_id : self.security_id,
                order_type: OrderType::Limit,
            },
            &match_span,
        );

        Ok(Response::new(NewOrderResponse {
            order_id: order_id.to_string(),
            status: 200,
            cause: None,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        Ok(Response::new(CancelOrderResponse {
            status: 200,
            cause: Some("".to_owned()),
        }))
    }
    async fn modify_order(
        &self,
        request: Request<ModifyOrderRequest>,
    ) -> Result<Response<ModifyOrderResponse>, Status> {
        Ok(Response::new(ModifyOrderResponse {
            status: 200,
            cause: Some("".to_owned()),
        }))
    }
    async fn book_depth(&self, request: Request<BookRequest>) -> Result<Response<BookResponse>, Status>{
        
        let match_span = Tracing::depth_span(
            Empty,
            Empty,
            Empty
        );

        let engine = self
            .engine
            .lock()
            .map_err(|_| Status::internal("engine mutex poisoned"))?;
        let req = request.get_ref();

        match engine.depth(self.security_id, req.level_count, &match_span){
            Ok(book_depth) => {
                let proto_depth = BookDepth::from(book_depth); // can directly use book_depth.into()
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

impl TradingServer {
    fn new() -> Self {
        Self {
            engine: Arc::new(Mutex::new(MatchingEngine::new())),
            security_id : Uuid::new_v4()
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

impl From<clob_engine::order_book::types::BookDepth> for BookDepth {
    fn from(value: clob_engine::order_book::types::BookDepth) -> Self {
        let bid_depth = value.bid_depth.into_iter()
        .map(|price_level_depth| PriceLevelDepth {
            price : price_level_depth.price_level,
            quantity : price_level_depth.quantity
        }).collect();

        let ask_depth = value.ask_depth.into_iter()
        .map(|price_level_depth| PriceLevelDepth {
            price : price_level_depth.price_level,
            quantity : price_level_depth.quantity
        }).collect();
        Self { bid_depth, ask_depth }
    }
}