
from data_ingestion.flows.ingestion_flow import market_data_flow, news_flow, end_of_day_flow, macro_flow
from data_ingestion.flows.health_monitor import health_monitor_flow
from prefect import serve

if __name__ == "__main__":
    print("Serving Phase 1 Ingestion Flows...")
    
    # Define deployments
    market_deployment = market_data_flow.to_deployment(
        name="Market-Data-Every-1min",
        cron="* 9-16 * * 1-5", # Market hours (approx)
    )
    
    news_deployment = news_flow.to_deployment(
        name="News-Every-5min",
        cron="*/5 * * * *",
    )
    
    health_deployment = health_monitor_flow.to_deployment(
        name="Health-Monitor-Every-10min",
        cron="*/10 * * * *",
    )

    # Serve all
    serve(
        market_deployment,
        news_deployment,
        health_deployment
    )
