# consumer.py - UPDATED WITH ALERT LOGGING
from confluent_kafka import Consumer
import json
import requests
import psycopg2
from datetime import datetime
from geopy.distance import geodesic
import math
import os
import csv  # ADD THIS

# ========== ADD THIS IMPORT ==========
try:
    from alerts_service import alert_service
    ALERT_SERVICE_AVAILABLE = True
    print("✅ Alert service loaded successfully")
except ImportError:
    ALERT_SERVICE_AVAILABLE = False
    print("⚠️ Alert service not available - running without alerts")
# =====================================

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'fraud_detection',
    'user': 'postgres',
    'password': '2005',
    'port': 5432
}

API_URL = "http://localhost:8001/predict"
TOPIC_NAME = "credit-card-transactions"

# ========== ADD ALERT THRESHOLD ==========
ALERT_THRESHOLD = 0.65  # 85% fraud score threshold for alerts
# =========================================

# Kafka Consumer Setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud-consumer-group',
    'auto.offset.reset': 'latest'
})

consumer.subscribe([TOPIC_NAME])

# ========== ADD SMS ALERT LOGGING FUNCTION ==========
def log_sms_alert(transaction_id, fraud_score, status="sent", alert_type="SMS"):
    """Log SMS alert to CSV file for dashboard"""
    try:
        log_file = "fraud_alerts.log"
        file_exists = os.path.exists(log_file)
        
        with open(log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'transaction_id', 'fraud_score', 'alert_type', 'status'])
            
            writer.writerow([
                datetime.now().isoformat(),
                transaction_id,
                round(fraud_score, 4),
                alert_type,
                status
            ])
        
        print(f"   📝 Alert logged: {transaction_id} - {status}")
        return True
    except Exception as e:
        print(f"   ❌ Alert logging failed: {e}")
        return False
# ====================================================

def connect_db():
    """Establish database connection"""
    return psycopg2.connect(**DB_CONFIG)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS coordinates in km"""
    R = 6371  # Earth's radius in km
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def calculate_travel_speed_kmh(lat1, lon1, time1_str, lat2, lon2, time2_str):
    """Calculate travel speed between two locations in km/h"""
    try:
        # Calculate distance
        distance_km = haversine_distance(lat1, lon1, lat2, lon2)
        
        # Calculate time difference
        time1 = datetime.fromisoformat(time1_str.replace('Z', '+00:00'))
        time2 = datetime.fromisoformat(time2_str.replace('Z', '+00:00'))
        time_diff_hours = abs((time2 - time1).total_seconds() / 3600)
        
        if time_diff_hours > 0:
            speed_kmh = distance_km / time_diff_hours
            return speed_kmh, distance_km, time_diff_hours
        else:
            return 0, distance_km, 0
    except:
        return 0, 0, 0

def save_to_db(tx, prediction):
    """Save transaction and prediction to database"""
    try:
        conn = connect_db()
        cur = conn.cursor()

        # Check if transaction already exists
        cur.execute("SELECT 1 FROM transactions WHERE transaction_id = %s", (tx["transaction_id"],))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute("""
                INSERT INTO transactions (
                    transaction_id, cc_number, user_id, amount, transaction_type,
                    city, card_lat, card_lon, merchant_lat, merchant_lon,
                    merchant_name, timestamp, hour_of_day, day_of_week,
                    is_night, is_fraud, predicted_fraud, fraud_score, fraud_reason
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                tx["transaction_id"],
                tx["cc_number"],
                tx["user_id"],
                tx["amount"],
                tx["transaction_type"],
                tx.get("city", "Unknown"),
                tx["card_lat"],
                tx["card_lon"],
                tx.get("merchant_lat", tx["card_lat"]),
                tx.get("merchant_lon", tx["card_lon"]),
                tx.get("merchant_name", "Unknown"),
                datetime.fromisoformat(tx["timestamp"].replace('Z', '+00:00')),
                tx["hour_of_day"],
                tx["day_of_week"],
                tx["is_night"],
                tx.get("is_fraud", 0),
                1 if prediction.get("is_fraud", False) else 0,
                prediction.get("fraud_score", 0),
                prediction.get("reason", "Unknown")
            ))
            conn.commit()
            print(f"💾 Saved to DB: {tx['transaction_id']}")
        else:
            print(f"⏭️  Already in DB: {tx['transaction_id']}")
            
        conn.close()

    except Exception as e:
        print(f"❌ Database Error: {e}")

def get_last_transaction(card_number):
    """Get last transaction GPS coordinates and timestamp"""
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT card_lat, card_lon, timestamp, city, amount 
            FROM transactions 
            WHERE cc_number = %s 
            ORDER BY timestamp DESC 
            LIMIT 1
        """, (card_number,))
        result = cur.fetchone()
        conn.close()
        
        if result:
            return {
                "card_lat": result[0],
                "card_lon": result[1],
                "timestamp": result[2].isoformat(),
                "city": result[3],
                "amount": result[4]
            }
        return None
    except Exception as e:
        print(f"❌ Error fetching last transaction: {e}")
        return None

def add_travel_info(tx):
    """Add travel analysis info to transaction for API"""
    last_tx = get_last_transaction(tx["cc_number"])
    
    if last_tx:
        # Calculate travel metrics
        speed_kmh, distance_km, time_hours = calculate_travel_speed_kmh(
            last_tx["card_lat"], last_tx["card_lon"], last_tx["timestamp"],
            tx["card_lat"], tx["card_lon"], tx["timestamp"]
        )
        
        # Add to transaction
        tx["last_transaction_time"] = last_tx["timestamp"]
        tx["last_lat"] = last_tx["card_lat"]
        tx["last_lon"] = last_tx["card_lon"]
        tx["last_city"] = last_tx["city"]
        tx["last_amount"] = last_tx["amount"]
        tx["distance_from_last_km"] = round(distance_km, 2)
        tx["time_since_last_hours"] = round(time_hours, 2)
        tx["travel_speed_kmh"] = round(speed_kmh, 2) if speed_kmh > 0 else 0
        
        # Detect unusual patterns
        if distance_km > 500 and time_hours < 2:
            tx["impossible_travel_risk"] = "HIGH"
        elif distance_km > 200 and time_hours < 1:
            tx["impossible_travel_risk"] = "VERY_HIGH"
        elif distance_km > 1000:
            tx["long_distance"] = True
        else:
            tx["impossible_travel_risk"] = "LOW"
    else:
        # First transaction for this card
        tx["first_transaction"] = True
        tx["distance_from_last_km"] = 0
        tx["time_since_last_hours"] = 0
        tx["travel_speed_kmh"] = 0
        tx["impossible_travel_risk"] = "LOW"
    
    return tx

print("\n" + "="*60)
print("   FRAUD DETECTION CONSUMER - GPS EDITION")
print("="*60)
print("📡 Listening for transactions...")
print("📍 Now with GPS coordinate analysis")
print("🚗 Travel speed calculation enabled")
# ========== ADD THIS LINE ==========
print("🔔 Real-time alerts enabled" if ALERT_SERVICE_AVAILABLE else "⚠️ Alerts disabled - service not found")
print(f"📝 SMS alert logging: ENABLED")
# ===================================
print("-"*60)

transaction_count = 0
fraud_count = 0
sms_alerts_sent = 0

try:
    while True:
        # Poll for messages
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # Parse transaction
        tx = json.loads(msg.value().decode('utf-8'))
        transaction_count += 1
        
        print(f"\n📥 Received Transaction #{transaction_count}")
        print(f"   ID: {tx['transaction_id'][-6:]}")
        print(f"   Card: {tx['cc_number'][-4:]} | User: {tx['user_id']}")
        print(f"   📍 {tx['card_lat']:.4f}, {tx['card_lon']:.4f} | 🏙️ {tx.get('city', 'Unknown')}")
        print(f"   💰 ₹{tx['amount']:,.2f} | 🏪 {tx.get('merchant_name', 'Unknown')}")
        
        # Add travel analysis
        tx_with_travel = add_travel_info(tx)
        
        # Show travel info if available
        if "distance_from_last_km" in tx_with_travel and tx_with_travel["distance_from_last_km"] > 0:
            print(f"   🚗 Distance from last: {tx_with_travel['distance_from_last_km']}km")
            print(f"   ⏱️  Time since last: {tx_with_travel['time_since_last_hours']:.1f}h")
            print(f"   🚀 Travel speed: {tx_with_travel['travel_speed_kmh']:.1f}km/h")
        
        # Call FastAPI for prediction
        try:
            response = requests.post(API_URL, json=tx_with_travel, timeout=3)
            prediction = response.json()
            
            if prediction.get("is_fraud", False):
                fraud_count += 1
                fraud_score = prediction.get('fraud_score', 0)
                print(f"   🚨 FRAUD DETECTED: {prediction.get('reason', 'Unknown')}")
                print(f"   🔴 Fraud Score: {fraud_score:.2%}")
                
                # ========== UPDATED ALERT TRIGGERING CODE ==========
                # Check if fraud and trigger alerts
                if ALERT_SERVICE_AVAILABLE and fraud_score > ALERT_THRESHOLD:
                    print(f"   🔔 Triggering SMS alert (Score: {fraud_score:.1%})")
                    
                    try:
                        alert_result = alert_service.trigger_alerts(tx, prediction)
                        
                        if alert_result.get("triggered"):
                            sms_alerts_sent += 1
                            print(f"   📱 SMS Alert: {'DEMO' if alert_result.get('mode') == 'demo' else 'SENT'}")
                            print(f"   📊 Alert Result: {alert_result.get('sms_result', {})}")
                            
                            # ========== LOG SMS ALERT TO FILE ==========
                            alert_status = "demo_sent" if alert_result.get('mode') == 'demo' else "sent"
                            log_sms_alert(tx['transaction_id'], fraud_score, alert_status, "SMS")
                            # ============================================
                            
                    except Exception as alert_error:
                        print(f"   ❌ Alert error: {alert_error}")
                        # Log failed alert
                        log_sms_alert(tx['transaction_id'], fraud_score, "failed", "SMS")
                else:
                    if fraud_score > ALERT_THRESHOLD:
                        # High fraud but no alert service available
                        print(f"   ⚠️ High fraud detected ({fraud_score:.1%}) but alert service unavailable")
                        log_sms_alert(tx['transaction_id'], fraud_score, "service_unavailable", "SMS")
                # =====================================================
                
            else:
                print(f"   ✅ Normal Transaction")
                print(f"   🟢 Fraud Score: {prediction.get('fraud_score', 0):.2%}")
                
        except Exception as e:
            print(f"❌ API Error: {e}")
            prediction = {
                "is_fraud": False, 
                "reason": "API ERROR",
                "fraud_score": 0.0
            }
        
        # Save to database
        save_to_db(tx, prediction)
        
        # Show summary
        print(f"   📊 Stats: {transaction_count} total, {fraud_count} fraud, {sms_alerts_sent} SMS alerts")
        print("-"*60)

except KeyboardInterrupt:
    print("\n\n🛑 Consumer stopped by user")
    print(f"📈 Final Stats:")
    print(f"   • Transactions processed: {transaction_count}")
    print(f"   • Fraud detected: {fraud_count}")
    print(f"   • SMS alerts sent: {sms_alerts_sent}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
finally:
    # Clean up
    consumer.close()
    print("✅ Consumer closed cleanly")