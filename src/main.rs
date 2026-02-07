use std::sync::{Arc, Mutex};
use clob_engine::MatchingEngine;
use tonic::{transport::Server, Request, Response,Status};
use orders::order_book_server::{OrderBook, OrderBookServer};
use orders::{CancelOrderRequest, CancelOrderResponse, NewOrderRequest, NewOrderResponse, ModifyOrderRequest, ModifyOrderResponse};

pub mod orders{
    tonic::include_proto!("orders");
}

#[derive(Debug)]
pub struct TradingServer{
    engine : Arc<Mutex<MatchingEngine>>
}

#[tonic::async_trait]
impl OrderBook for TradingServer {
    async fn new_order(&self, request : Request<NewOrderRequest>) -> Result<Response<NewOrderResponse>, Status>{
        Ok(Response::new(NewOrderResponse { status: 200, cause : Some("".to_owned()) }))
    }
    async fn cancel_order(&self, request : Request<CancelOrderRequest>) -> Result<Response<CancelOrderResponse>, Status>{
        Ok(Response::new(CancelOrderResponse {status : 200, cause : Some("".to_owned())}))
    }
    async fn modify_order(&self, request : Request<ModifyOrderRequest>) -> Result<Response<ModifyOrderResponse>,Status>{
        Ok(Response::new(ModifyOrderResponse { status: 200, cause: Some("".to_owned()) }))
    }
}

impl TradingServer {
    fn new() -> Self{
        Self { engine: Arc::new(Mutex::new(MatchingEngine::new())) }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let address = "[::1]:50051".parse()?;
    let trading_server = TradingServer::new();

    Server::builder()
    .add_service(OrderBookServer::new(trading_server)) //orderbook server is a wrapper, which 'listen for incoming grpc calls', 'deserialize protobuf message', 'call trading server methods','serialize response back'.
    .serve(address)
    .await;

    Ok(())
}
