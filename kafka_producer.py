import time
import json
import random
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import requests


"""
env = load_dotenv('./.env')

api_key = os.getenv('ALPHAVANTAGE')
"""

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

stocks = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NFLX", "NVDA", "AMD", "IBM"]

stock_prices = {stock: random.uniform(100, 500) for stock in stocks}

while True:
    timestamp = time.time()  # Capture the timestamp once per iteration
    stock_data = []

    for stock in stocks:
        # Simulate price fluctuation
        change = random.uniform(-2, 2)  # Random change between -2 and 2
        stock_prices[stock] += change
        stock_prices[stock] = max(stock_prices[stock], 1)  # Ensure price doesn't go negative

        # Store data for batch send
        stock_data.append({"stock": stock, "price": stock_prices[stock], "timestamp": timestamp})

    # Push all stock data at the same timestamp to Kafka
    for data in stock_data:
        producer.send("stock_topic", value=data)
        print(f"data: {data}")

    time.sleep(1)  # Update every second


"""

while True:
    for 
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={api_key}"
    response = requests.get(url).json()
    latest_price = list(response['Time Series (1min)'].values())[0]['1. open']
    
    producer.send('stock-prices', {'ticker': ticker, 'price': latest_price})
    time.sleep(60)
"""