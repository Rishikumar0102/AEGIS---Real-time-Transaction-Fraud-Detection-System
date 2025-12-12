# producer.py - UPDATED VERSION WITH LATITUDE/LONGITUDE
import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
from geopy.distance import geodesic

# Kafka Setup
producer = Producer({'bootstrap.servers': 'localhost:9092'})
TOPIC = "credit-card-transactions"

# India major cities with coordinates
CITIES = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Chennai": (13.0827, 80.2707),
    "Bangalore": (12.9716, 77.5946),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867),
    "Pune": (18.5204, 73.8567),
    "Ahmedabad": (23.0225, 72.5714),
    "Jaipur": (26.9124, 75.7873),
    "Lucknow": (26.8467, 80.9462),
    "Chandigarh": (30.7333, 76.7794),
    "Dehradun": (30.3165, 78.0322),
    "Bhopal": (23.2599, 77.4126),
    "Indore": (22.7196, 75.8577),
    "Nagpur": (21.1458, 79.0882)
}

# India bounding coordinates
INDIA_BOUNDS = {
    "min_lat": 8.0,   # Southern tip
    "max_lat": 37.0,  # Northern tip
    "min_lon": 68.0,  # Western edge
    "max_lon": 97.0   # Eastern edge
}

# Generate realistic credit card profiles with home coordinates
credit_cards = []
for _ in range(200):
    home_city = random.choice(list(CITIES.keys()))
    base_lat, base_lon = CITIES[home_city]
    
    # Add small random offset to each card's "home" location (within 10km)
    card_lat = base_lat + random.uniform(-0.09, 0.09)
    card_lon = base_lon + random.uniform(-0.09, 0.09)
    
    credit_cards.append({
        "cc_number": f"4{random.randint(100,999):03d}****{random.randint(1000,9999):04d}",
        "user_id": f"USER{random.randint(10000,99999)}",
        "credit_limit": random.choice([20000, 40000, 60000, 100000, 150000]),
        "home_city": home_city,
        "home_lat": card_lat,
        "home_lon": card_lon
    })


def get_closest_city(lat, lon):
    """Find the closest city name from coordinates."""
    closest_city = "Unknown"
    min_distance = float('inf')
    
    for city, (city_lat, city_lon) in CITIES.items():
        distance = geodesic((lat, lon), (city_lat, city_lon)).km
        if distance < min_distance:
            min_distance = distance
            closest_city = city
    
    # If very far from any known city, show approximate region
    if min_distance > 100:
        if lat > 30:
            closest_city = "North India"
        elif lat < 15:
            closest_city = "South India"
        elif lon < 75:
            closest_city = "West India"
        else:
            closest_city = "East India"
    
    return closest_city


def send_transaction(transaction):
    """Send transaction to Kafka topic."""
    producer.produce(TOPIC, key=transaction["cc_number"], value=json.dumps(transaction))
    producer.flush()
    print(f" Sent → ID: {transaction['transaction_id'][-6:]} | "
          f" {transaction['card_lat']:.4f}°N, {transaction['card_lon']:.4f}°E | "
          f" {transaction['cc_number'][-4:]} | "
          f" ₹{transaction['amount']:,.2f} | "
          f" {transaction['city']}")


def generate_random_transaction():
    """Generate realistic transaction with GPS coordinates."""
    
    card = random.choice(credit_cards)
    timestamp = datetime.now().isoformat()
    
    # Base coordinates from card's home location
    base_lat, base_lon = card["home_lat"], card["home_lon"]
    
    # Fraud chance logic (5% fraud rate)
    fraud_trigger = random.random() < 0.05
    is_night = 1 if datetime.now().hour < 6 or datetime.now().hour > 22 else 0
    
    if fraud_trigger:
        # FRAUD: Generate random coordinates anywhere in India
        lat = random.uniform(INDIA_BOUNDS["min_lat"], INDIA_BOUNDS["max_lat"])
        lon = random.uniform(INDIA_BOUNDS["min_lon"], INDIA_BOUNDS["max_lon"])
        
        # Fraud transactions often have unusual amounts
        amount_options = [
            round(random.uniform(15000, min(100000, card["credit_limit"] * 0.8)), 2),  # Large purchase
            round(random.uniform(5000, 20000), 2),  # Medium but suspicious
            round(random.uniform(1, 100), 2)  # Small test transaction
        ]
        amount = random.choice(amount_options)
        
        # Fraud often happens at night or unusual times
        if random.random() < 0.7:
            is_night = 1
        
    else:
        # NORMAL: Generate coordinates near home location (within 100km)
        max_offset = 0.9  # Approximately 100km
        lat = base_lat + random.uniform(-max_offset, max_offset)
        lon = base_lon + random.uniform(-max_offset, max_offset)
        
        # Keep within India bounds
        lat = max(INDIA_BOUNDS["min_lat"], min(INDIA_BOUNDS["max_lat"], lat))
        lon = max(INDIA_BOUNDS["min_lon"], min(INDIA_BOUNDS["max_lon"], lon))
        
        # Realistic transaction amounts
        amount_categories = [
            ("small", 0.7, (50, 2000)),      # 70% small transactions
            ("medium", 0.25, (2000, 15000)), # 25% medium transactions
            ("large", 0.05, (15000, 50000))  # 5% large transactions
        ]
        
        category = random.choices(
            [cat[0] for cat in amount_categories],
            weights=[cat[1] for cat in amount_categories]
        )[0]
        
        for cat_name, prob, (min_amt, max_amt) in amount_categories:
            if category == cat_name:
                amount = round(random.uniform(min_amt, max_amt), 2)
                break
    
    # Find closest city for display
    city = get_closest_city(lat, lon)
    
    # Merchant location (slightly different from card location - within 1km)
    merchant_lat = lat + random.uniform(-0.009, 0.009)
    merchant_lon = lon + random.uniform(-0.009, 0.009)
    
    # Transaction type based on amount and time
    if amount < 1000:
        txn_type = random.choice(['POS', 'ONLINE'])
    elif amount > 20000:
        txn_type = 'ONLINE' if random.random() < 0.7 else 'POS'
    else:
        txn_type = random.choice(['ATM', 'POS', 'ONLINE'])
    
    # Special merchant names for fraud
    if fraud_trigger:
        merchant_names = ['IntlMerchant', 'UnknownVendor', 'SuspiciousStore', 'TestMerchant']
    else:
        merchant_names = ['Amazon', 'Flipkart', 'Swiggy', 'Zomato', 'DMart', 'Reliance', 
                         'BPCL', 'IOCL', 'BigBasket', 'Netflix', 'Spotify']
    
    return {
        "transaction_id": f"TXN{uuid.uuid4().hex[:8].upper()}",
        "cc_number": card["cc_number"],
        "user_id": card["user_id"],
        "credit_limit": card["credit_limit"],
        "amount": amount,
        "transaction_type": txn_type,
        "city": city,
        "card_lat": round(lat, 6),
        "card_lon": round(lon, 6),
        "merchant_lat": round(merchant_lat, 6),
        "merchant_lon": round(merchant_lon, 6),
        "merchant_name": random.choice(merchant_names),
        "timestamp": timestamp,
        "hour_of_day": datetime.now().hour,
        "day_of_week": datetime.now().weekday(),
        "is_night": is_night,
        "is_fraud": 1 if fraud_trigger else 0
    }


def manual_input_transaction():
    """Manual transaction entry with latitude/longitude."""
    print("\n" + "="*50)
    print("📍 MANUAL TRANSACTION ENTRY (GPS COORDINATES)")
    print("="*50)
    
    # Card details
    cc = input("\n💳 Card Number (e.g., 4123****5678): ").strip()
    if not cc:
        cc = f"4{random.randint(100,999):03d}****{random.randint(1000,9999):04d}"
    
    user = input("👤 User ID (e.g., USER12345): ").strip()
    if not user:
        user = f"USER{random.randint(10000,99999)}"
    
    # Transaction details
    try:
        amount = float(input(" Amount (₹): "))
    except:
        amount = round(random.uniform(100, 5000), 2)
        print(f"  Using random amount: ₹{amount:,.2f}")
    
    txn_type = input("💼 Transaction Type (ATM/POS/ONLINE): ").upper().strip()
    if txn_type not in ['ATM', 'POS', 'ONLINE']:
        txn_type = random.choice(['ATM', 'POS', 'ONLINE'])
        print(f"  Using random type: {txn_type}")
    
    # GPS Coordinates
    print("\n ENTER GPS COORDINATES")
    print("   Example: Delhi - 28.6139, 77.2090")
    print("           Mumbai - 19.0760, 72.8777")
    print("           Chennai - 13.0827, 80.2707")
    
    while True:
        try:
            lat_input = input(" Latitude (e.g., 28.6139): ").strip()
            lon_input = input(" Longitude (e.g., 77.2090): ").strip()
            
            # If empty, use random Indian coordinates
            if not lat_input or not lon_input:
                lat = random.uniform(INDIA_BOUNDS["min_lat"], INDIA_BOUNDS["max_lat"])
                lon = random.uniform(INDIA_BOUNDS["min_lon"], INDIA_BOUNDS["max_lon"])
                print(f"  Using random coordinates: {lat:.4f}, {lon:.4f}")
            else:
                lat = float(lat_input)
                lon = float(lon_input)
            
            # Validate coordinates are within India
            if not (INDIA_BOUNDS["min_lat"] <= lat <= INDIA_BOUNDS["max_lat"] and 
                    INDIA_BOUNDS["min_lon"] <= lon <= INDIA_BOUNDS["max_lon"]):
                print(f" Coordinates outside India bounds!")
                print(f"   Must be: Lat {INDIA_BOUNDS['min_lat']} to {INDIA_BOUNDS['max_lat']}")
                print(f"            Lon {INDIA_BOUNDS['min_lon']} to {INDIA_BOUNDS['max_lon']}")
                continue
            
            break
        except ValueError:
            print(" Invalid coordinates! Please enter numbers only.")
        except Exception as e:
            print(f" Error: {e}")
    
    # Find city and generate merchant location
    city = get_closest_city(lat, lon)
    merchant_lat = lat + random.uniform(-0.005, 0.005)  # Within 500m
    merchant_lon = lon + random.uniform(-0.005, 0.005)
    
    # Merchant name
    merchant = input(" Merchant Name ( Enter for random): ").strip()
    if not merchant:
        merchant = random.choice(['Amazon', 'Flipkart', 'Swiggy', 'DMart', 'Reliance', 'LocalStore'])
    
    # Credit limit (default or input)
    try:
        credit_limit = float(input("Credit Limit (₹, press Enter for 50000): ") or "50000")
    except:
        credit_limit = 50000
    
    # Create transaction
    return {
        "transaction_id": f"TXN{uuid.uuid4().hex[:8].upper()}",
        "cc_number": cc,
        "user_id": user,
        "credit_limit": credit_limit,
        "amount": amount,
        "transaction_type": txn_type,
        "city": city,
        "card_lat": round(lat, 6),
        "card_lon": round(lon, 6),
        "merchant_lat": round(merchant_lat, 6),
        "merchant_lon": round(merchant_lon, 6),
        "merchant_name": merchant,
        "timestamp": datetime.now().isoformat(),
        "hour_of_day": datetime.now().hour,
        "day_of_week": datetime.now().weekday(),
        "is_night": 1 if datetime.now().hour < 6 or datetime.now().hour > 22 else 0,
        "is_fraud": 0  # Manual entries are assumed legitimate
    }


def show_coordinate_examples():
    """Show example coordinates for major cities."""
    print("\n" + "="*50)
    print("📍 EXAMPLE COORDINATES FOR MAJOR INDIAN CITIES")
    print("="*50)
    print(f"{'City':<15} {'Latitude':<12} {'Longitude':<12}")
    print("-"*50)
    
    for city, (lat, lon) in CITIES.items():
        print(f"{city:<15} {lat:<12.4f} {lon:<12.4f}")
    
    print("\nOther locations in India:")
    print("Goa Beach:       15.2993, 74.1240")
    print("Kashmir:         34.0837, 74.7973")
    print("Kerala Backwaters:9.4981, 76.3388")
    print("Rajasthan Desert:27.0238, 74.2179")
    print("="*50)


def main_menu():
    """Main producer menu."""
    while True:
        print("\n" + "="*60)
        print("           FRAUD DETECTION - TRANSACTION PRODUCER")
        print("="*60)
        print("1.  Continuous Streaming Mode")
        print("   • Generates transactions every 3 seconds")
        print("   • Uses all the  GPS coordinates for all transactions")   
        print()
        print("2.  Manual Entry Mode")
        print("   • Enter transaction details manually")
        print("   • Input latitude/longitude coordinates")
        print("   • Supports random coordinates if needed")
        print()
        print("4. Exit Producer")
        print("="*60)
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == "1":
            print("\n" + "="*60)
            print("🚀 STARTING CONTINUOUS STREAMING")
            print("="*60)
            print("• Normal transactions: Near home coordinates")
            print("• Fraud transactions: Random India coordinates")
            print("• Streaming every 3 seconds...")
            print("="*60 + "\n")
            
            try:
                counter = 0
                while True:
                    transaction = generate_random_transaction()
                    send_transaction(transaction)
                    counter += 1
                    
                    # Show summary every 10 transactions
                    if counter % 10 == 0:
                        print(f"\n Streamed {counter} transactions... (Ctrl+C to stop)\n")
                    
                    time.sleep(3)
            except KeyboardInterrupt:
                print("\n\n  Streaming stopped by user")
                print(f"Total transactions streamed: {counter}")
                continue
        
        elif choice == "2":
            transaction = manual_input_transaction()
            print("\n" + "="*50)
            print("✅ TRANSACTION READY TO SEND")
            print("="*50)
            print(f"ID: {transaction['transaction_id']}")
            print(f"Card: {transaction['cc_number']}")
            print(f"User: {transaction['user_id']}")
            print(f"Amount: ₹{transaction['amount']:,.2f}")
            print(f"Location: {transaction['card_lat']:.4f}, {transaction['card_lon']:.4f}")
            print(f"City: {transaction['city']}")
            print("="*50)
            
            confirm = input("\nSend this transaction? (y/n): ").lower()
            if confirm == 'y':
                send_transaction(transaction)
                print(" Transaction sent successfully!")
            else:
                print(" Transaction cancelled")
        
        elif choice == "3":
            show_coordinate_examples()
        
        elif choice == "4":
            print("\n👋 Exiting Producer. Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    print("\n" + "✨" * 30)
    print("   REAL-TIME FRAUD DETECTION SYSTEM")
    print("        GPS COORDINATES EDITION")
    print("✨" * 30)
    
    
    print(f"\n📊 Loaded {len(credit_cards)} credit card profiles")
    print(f"📍 Using {len(CITIES)} Indian cities as references")
    print(f"🌏 India bounds: {INDIA_BOUNDS['min_lat']}° to {INDIA_BOUNDS['max_lat']}°N, "
          f"{INDIA_BOUNDS['min_lon']}° to {INDIA_BOUNDS['max_lon']}°E")
    
    main_menu()