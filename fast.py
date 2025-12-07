# fastapi_fraud.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List
import numpy as np
import pandas as pd
import joblib
import json
import math
from datetime import datetime
import os

# Disable warnings
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import warnings
warnings.filterwarnings('ignore')

app = FastAPI()

# Request Model - Matches your producer
class TransactionRequest(BaseModel):
    transaction_id: str
    cc_number: str
    user_id: str
    credit_limit: float
    amount: float
    transaction_type: str
    city: str
    card_lat: float
    card_lon: float
    merchant_lat: float
    merchant_lon: float
    timestamp: str
    hour_of_day: int
    day_of_week: int
    is_night: int
    is_fraud: int = 0
    last_transaction_time: Optional[str] = None
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None

class FraudPrediction(BaseModel):
    is_fraud: bool
    fraud_score: float
    reason: str

# ========== LOAD MODEL ==========
print("🚀 Loading model...")

# Try loading 002 files first, then fallback to original
model = None
scaler = None
threshold = None
feature_columns = []

try:
    from tensorflow import keras
    
    # Try loading model with 002 suffix
    try:
        model = keras.models.load_model("fraud_detection_model_002.keras")
        print("✅ Model loaded: fraud_detection_model_002.keras")
    except:
        try:
            model = keras.models.load_model("fraud_detection_model_002.h5")
            print("✅ Model loaded: fraud_detection_model_002.h5")
        except:
            try:
                model = keras.models.load_model("fraud_detection_model.keras")
                print("✅ Model loaded: fraud_detection_model.keras")
            except:
                try:
                    model = keras.models.load_model("fraud_detection_model.h5")
                    print("✅ Model loaded: fraud_detection_model.h5")
                except Exception as e:
                    print(f"❌ Model loading failed: {e}")
                    
except Exception as e:
    print(f"❌ TensorFlow error: {e}")

# Load scaler
try:
    scaler = joblib.load("fraud_detection_model_002_scaler.pkl")
    print("✅ Scaler loaded")
except:
    try:
        scaler = joblib.load("fraud_detection_model_scaler.pkl")
        print("✅ Scaler loaded (fallback)")
    except:
        print("❌ Scaler not found")
        scaler = None

# Load threshold
try:
    threshold = joblib.load("fraud_detection_model_002_threshold.pkl")
    print(f"✅ Threshold loaded: {threshold:.4f}")
except:
    try:
        threshold = joblib.load("fraud_detection_model_threshold.pkl")
        print(f"✅ Threshold loaded (fallback): {threshold:.4f}")
    except:
        print("❌ Threshold not found, using 0.5")
        threshold = 0.5

# Load config
try:
    with open("fraud_detection_model_002_config.json", "r") as f:
        config = json.load(f)
    feature_columns = config.get('feature_columns', [])
    print(f"✅ Config loaded, {len(feature_columns)} features")
except:
    try:
        with open("fraud_detection_model_config.json", "r") as f:
            config = json.load(f)
        feature_columns = config.get('feature_columns', [])
        print(f"✅ Config loaded (fallback), {len(feature_columns)} features")
    except:
        print("❌ Config not found")
        feature_columns = []

# ========== FEATURE PREPARATION ==========
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance using Haversine formula"""
    R = 6371
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def prepare_features(transaction: TransactionRequest):
    """Prepare features for model"""
    
    features = {}
    
    # Amount features
    features['amount_to_limit'] = transaction.amount / transaction.credit_limit
    features['amount_log'] = math.log1p(transaction.amount)
    features['is_large_amount'] = 1.0 if transaction.amount > transaction.credit_limit * 0.3 else 0.0
    
    # Time features
    def get_hour_bin(hour):
        if 6 <= hour < 12: return 0.0
        elif 12 <= hour < 18: return 1.0
        elif 18 <= hour < 22: return 2.0
        else: return 3.0
    
    features['hour_bin'] = float(get_hour_bin(transaction.hour_of_day))
    features['hour_sin'] = math.sin(2 * math.pi * transaction.hour_of_day / 24)
    features['hour_cos'] = math.cos(2 * math.pi * transaction.hour_of_day / 24)
    features['day_sin'] = math.sin(2 * math.pi * transaction.day_of_week / 7)
    features['day_cos'] = math.cos(2 * math.pi * transaction.day_of_week / 7)
    
    # Location features
    distance = haversine_distance(
        transaction.card_lat, transaction.card_lon,
        transaction.merchant_lat, transaction.merchant_lon
    )
    features['merchant_distance_km'] = distance
    features['distance_log'] = math.log1p(distance)
    
    # Risk features
    features['is_night'] = float(transaction.is_night)
    features['is_high_risk_time'] = float(
        (transaction.is_night == 1) or 
        (transaction.hour_of_day < 6) or 
        (transaction.hour_of_day > 22)
    )
    
    # Transaction type
    txn_type = transaction.transaction_type.upper()
    features['txn_ATM'] = 1.0 if txn_type == 'ATM' else 0.0
    features['txn_POS'] = 1.0 if txn_type == 'POS' else 0.0
    features['txn_ONLINE'] = 1.0 if txn_type == 'ONLINE' else 0.0
    
    # Create DataFrame
    df = pd.DataFrame([features])
    
    # Add missing columns
    for col in feature_columns:
        if col not in df.columns:
            df[col] = 0.0
    
    # Reorder columns
    if feature_columns:
        X = df[feature_columns].fillna(0).values
    else:
        X = df.fillna(0).values
    
    return X

# In fastapi_fraud.py, update the rule_based_detection function:

def rule_based_detection(transaction: TransactionRequest):
    """Enhanced rule-based fraud detection with GPS analysis"""
    fraud_score = 0.0
    reasons = []
    
    # High amount relative to limit
    amount_ratio = transaction.amount / transaction.credit_limit
    if amount_ratio > 0.7:
        fraud_score += 0.5
        reasons.append("Very high amount (>70% of limit)")
    elif amount_ratio > 0.5:
        fraud_score += 0.3
        reasons.append("High amount (>50% of limit)")
    elif amount_ratio > 0.3:
        fraud_score += 0.1
        reasons.append("Moderate amount (>30% of limit)")
    
    # Night transaction
    if transaction.is_night == 1:
        fraud_score += 0.2
        reasons.append("Night transaction")
    
    # ATM at night (high risk)
    if transaction.transaction_type.upper() == 'ATM' and transaction.is_night == 1:
        fraud_score += 0.4
        reasons.append("ATM withdrawal at night")
    
    # Large merchant distance (card not present at merchant)
    merchant_distance = haversine_distance(
        transaction.card_lat, transaction.card_lon,
        transaction.merchant_lat, transaction.merchant_lon
    )
    if merchant_distance > 100:
        fraud_score += 0.3
        reasons.append(f"Large merchant distance ({merchant_distance:.1f}km)")
    
    # Impossible travel detection (using GPS)
    if hasattr(transaction, 'last_lat') and transaction.last_lat and transaction.last_lon:
        try:
            distance_km = haversine_distance(
                transaction.last_lat, transaction.last_lon,
                transaction.card_lat, transaction.card_lon
            )
            
            if hasattr(transaction, 'last_transaction_time') and transaction.last_transaction_time:
                current_time = datetime.fromisoformat(transaction.timestamp.replace('Z', '+00:00'))
                last_time = datetime.fromisoformat(transaction.last_transaction_time.replace('Z', '+00:00'))
                time_diff_min = (current_time - last_time).total_seconds() / 60
                
                # Calculate minimum required travel time (assuming 800 km/h max)
                min_required_time = distance_km / 800  # hours
                min_required_min = min_required_time * 60
                
                if time_diff_min < min_required_min:
                    # Impossible travel
                    fraud_score += 0.6
                    reasons.append(f"Impossible travel: {distance_km:.1f}km in {time_diff_min:.1f}min")
                elif time_diff_min < 120 and distance_km > 500:
                    # Suspiciously fast travel
                    fraud_score += 0.4
                    reasons.append(f"Rapid travel: {distance_km:.1f}km in {time_diff_min:.1f}min")
                elif distance_km > 1000:
                    # Very long distance
                    fraud_score += 0.2
                    reasons.append(f"Long distance: {distance_km:.1f}km")
                    
        except Exception as e:
            print(f"⚠️ Travel analysis error: {e}")
    
    # Multiple transactions in short time
    if hasattr(transaction, 'time_since_last_hours') and transaction.time_since_last_hours < 0.1:
        fraud_score += 0.2
        reasons.append("Rapid consecutive transactions")
    
    # First transaction for card (higher risk)
    if hasattr(transaction, 'first_transaction') and transaction.first_transaction:
        if transaction.amount > 10000:
            fraud_score += 0.3
            reasons.append("Large first transaction")
    
    return min(1.0, fraud_score), reasons

# ========== MAIN PREDICTION ==========
@app.post("/predict", response_model=FraudPrediction)
async def predict(transaction: TransactionRequest):
    """Predict if transaction is fraudulent"""
    
    try:
        print(f"📥 Processing: {transaction.transaction_id}")
        
        # Rule-based detection
        rule_score, rule_reasons = rule_based_detection(transaction)
        
        # Model-based detection
        model_score = 0.0
        model_reason = ""
        
        if model is not None and scaler is not None:
            try:
                X = prepare_features(transaction)
                X_scaled = scaler.transform(X)
                reconstructions = model.predict(X_scaled, verbose=0)
                mse = np.mean(np.power(X_scaled - reconstructions, 2))
                
                if threshold > 0:
                    model_score = min(1.0, mse / threshold)
                else:
                    model_score = mse
                
                if mse > threshold:
                    model_reason = "Anomaly detected"
                    model_score = 0.7
                
            except Exception as e:
                print(f"⚠️ Model error: {e}")
        
        # Combine scores
        if model is not None:
            final_score = (model_score * 0.7) + (rule_score * 0.3)
        else:
            final_score = rule_score
        
        # Determine fraud
        is_fraud = final_score > 0.5
        
        # Prepare reason
        if is_fraud:
            if model_reason:
                reason = model_reason
            elif rule_reasons:
                reason = " | ".join(rule_reasons)
            else:
                reason = "High fraud score"
        else:
            reason = "Normal transaction"
        
        return FraudPrediction(
            is_fraud=is_fraud,
            fraud_score=min(1.0, final_score),
            reason=reason
        )
        
    except Exception as e:
        print(f"❌ Prediction error: {e}")
        return FraudPrediction(
            is_fraud=False,
            fraud_score=0.0,
            reason=f"Error: {str(e)[:50]}"
        )

@app.get("/")
def root():
    return {
        "message": "Fraud Detection API",
        "status": "running",
        "model_loaded": model is not None,
        "scaler_loaded": scaler is not None
    }

if __name__ == "__main__":
    import uvicorn
    print("\n" + "="*50)
    print("🚀 Fraud Detection API Starting...")
    print(f"📊 Model loaded: {model is not None}")
    print(f"📈 Scaler loaded: {scaler is not None}")
    print(f"🎯 Threshold: {threshold}")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8001)