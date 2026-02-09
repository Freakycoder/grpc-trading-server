use orders::order_book_client::OrderBookClient;
use orders::NewOrderRequest;
use tonic::Request;

pub mod orders{
    tonic::include_proto!("orders");
}
#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    let mut client = OrderBookClient::connect("http://[::1]:50051").await?;
    let request = Request::new(NewOrderRequest{
        user_id : "fwikfbw342929".to_owned(),
        price : Some(35),
        quantity : 400,
        is_buy_side : true,
        security_id : "BTC".to_owned(),
        order_type : 0
    });
    let response = client.new_order(request).await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}