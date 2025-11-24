import requests
import json
import time
import os
from kafka import KafkaProducer
from datetime import datetime

KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '10'))

# Multiple cryptocurrency pairs for diverse data
CRYPTO_PAIRS = [
    'BTC-USD',   # Bitcoin
    'ETH-USD',   # Ethereum
    'SOL-USD',   # Solana
    'ADA-USD',   # Cardano
    'DOGE-USD',  # Dogecoin
]

COINBASE_SPOT_API = 'https://api.coinbase.com/v2/prices/{pair}/spot'

def create_kafka_producer():
    """
    Tạo và trả về một Kafka Producer.
    Tự động retry nếu Kafka chưa sẵn sàng.
    """
    print("Đang thử kết nối tới Kafka...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                retries=5,
                acks='all'
            )
            print("Đã kết nối tới Kafka thành công!")
            return producer
        except Exception as e:
            print(f"Không thể kết nối tới Kafka: {e}. Đang thử lại sau 5 giây...")
            time.sleep(5)

def fetch_data_and_produce():
    """
    Lấy dữ liệu từ Coinbase cho nhiều crypto pairs và gửi vào Kafka topic.
    """
    producer = create_kafka_producer()
    
    print(f"🚀 Bắt đầu lấy dữ liệu từ Coinbase API")
    print(f"📊 Tracking {len(CRYPTO_PAIRS)} cryptocurrency pairs: {', '.join(CRYPTO_PAIRS)}")
    print(f"📤 Sending to Kafka topic: '{KAFKA_TOPIC}'")
    print("=" * 80)
    
    iteration = 0
    while True:
        iteration += 1
        timestamp = int(time.time())
        success_count = 0
        
        print(f"\n[Iteration {iteration}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        for pair in CRYPTO_PAIRS:
            try:
                # Fetch spot price
                api_url = COINBASE_SPOT_API.format(pair=pair)
                response = requests.get(api_url, timeout=5)
                response.raise_for_status()
                
                data = response.json()
                price_data = data.get('data')
                
                if price_data and 'amount' in price_data:
                    # Create enriched record with volume simulation
                    # (Coinbase API v2 doesn't provide volume, simulating for demo)
                    record = {
                        'timestamp': timestamp,
                        'symbol': f"{price_data['base']}-{price_data['currency']}",
                        'base': price_data['base'],
                        'currency': price_data['currency'],
                        'price': float(price_data['amount']),
                        'volume_24h': None,  # Will be None for now
                        'source': 'coinbase',
                        'iteration': iteration
                    }
                    
                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, key=record['symbol'], value=record)
                    
                    success_count += 1
                    print(f"  ✅ {record['symbol']:<12} Price: ${record['price']:>12,.2f}")
                    
                else:
                    print(f"  ⚠️  {pair:<12} Invalid response format")

            except requests.exceptions.RequestException as e:
                print(f"  ❌ {pair:<12} API Error: {e}")
            except Exception as e:
                print(f"  ❌ {pair:<12} Error: {e}")
        
        # Flush all messages
        producer.flush()
        
        print(f"\n📊 Summary: {success_count}/{len(CRYPTO_PAIRS)} pairs sent successfully")
        print(f"⏳ Waiting {POLL_INTERVAL_SECONDS} seconds...")
        print("=" * 80)
        
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    fetch_data_and_produce()