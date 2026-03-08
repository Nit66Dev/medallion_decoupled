import socket
import time
import json
import random
from datetime import datetime

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 9999))
    server.listen(1)
    print("Cybertron-Mart Server running on port 9999. Waiting for PySpark to connect...")

    conn, addr = server.accept()
    print(f"Connected by {addr}. Streaming data...")

    categories = ["Electronics", "Clothing", "Toys", None]
    events = ["click", "add_to_cart", "purchase"]

    try:
        while True:
            # Simulate dirty data: sometimes price is missing, sometimes category is null
            payload = {
                "user_id": random.choice([101, 102, 103, 104, None]),
                "event_type": random.choice(events),
                "category": random.choice(categories),
                "price": round(random.uniform(10.0, 500.0), 2) if random.random() > 0.2 else None,
                "event_timestamp": datetime.now().isoformat()
            }
            # Convert dict to JSON string and add a newline character
            msg = json.dumps(payload) + "\n"
            conn.sendall(msg.encode('utf-8'))
            print(f"Sent: {msg.strip()}")
            time.sleep(2) # Send a new event every 2 seconds
    except Exception as e:
        print(f"Connection closed: {e}")
    finally:
        conn.close()
        server.close()

if __name__ == "__main__":
    start_server()
