# debug_all.py
import psycopg2
import requests
from datetime import datetime

print("="*60)
print("   DEBUGGING FRAUD DETECTION SYSTEM")
print("="*60)

# Database config
DB_CONFIG = {
    "host": "localhost",
    "database": "fraud_detection",
    "user": "postgres",
    "password": "2005",
    "port": 5432
}

def test_database():
    print("\n🔍 Testing Database Connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Test connection
        cur.execute("SELECT version()")
        db_version = cur.fetchone()[0]
        print(f"✅ PostgreSQL Connected: {db_version[:50]}...")
        
        # List all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        print(f"📋 Tables in database: {[t[0] for t in tables]}")
        
        # Check transactions table
        if 'transactions' in [t[0] for t in tables]:
            # Get row count
            cur.execute("SELECT COUNT(*) FROM transactions")
            row_count = cur.fetchone()[0]
            print(f"📊 Rows in transactions table: {row_count}")
            
            # Show sample
            if row_count > 0:
                cur.execute("SELECT * FROM transactions LIMIT 3")
                rows = cur.fetchall()
                print("📝 Sample rows:")
                for row in rows:
                    print(f"   {row}")
            else:
                print("❌ Transactions table is EMPTY!")
                
            # Show columns
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'transactions'
            """)
            columns = cur.fetchall()
            print("🗂️ Table columns:")
            for col in columns:
                print(f"   {col[0]} ({col[1]})")
        else:
            print("❌ 'transactions' table does not exist!")
            
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return False

def test_fastapi():
    print("\n🔍 Testing FastAPI...")
    try:
        # Test root endpoint
        response = requests.get("http://localhost:8001/", timeout=5)
        print(f"✅ FastAPI is running: {response.status_code}")
        print(f"   Response: {response.json()}")
        
        # Test stats endpoint
        response = requests.get("http://localhost:8001/stats", timeout=5)
        print(f"✅ Stats endpoint: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Data: {data}")
        else:
            print(f"❌ Stats error: {response.text}")
            
        return True
        
    except requests.ConnectionError:
        print("❌ FastAPI is NOT running on http://localhost:8001")
        print("   Run: python fastapi_server.py")
        return False
    except Exception as e:
        print(f"❌ API Error: {e}")
        return False

def create_fix_database():
    print("\n🛠️ Creating/Fixing Database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(100) UNIQUE,
                cc_number VARCHAR(50),
                user_id VARCHAR(50),
                amount DECIMAL(10,2),
                city VARCHAR(100),
                timestamp TIMESTAMP,
                predicted_fraud INTEGER DEFAULT 0,
                fraud_score DECIMAL(10,6),
                fraud_reason TEXT
            )
        """)
        
        # Add sample data
        sample_data = [
            ("TXN001", "4111****1111", "USER001", 5000.00, "Delhi", datetime.now(), 0, 0.1, "Normal"),
            ("TXN002", "4222****2222", "USER002", 45000.00, "Mumbai", datetime.now(), 1, 0.8, "High amount"),
            ("TXN003", "4333****3333", "USER003", 1000.00, "Chennai", datetime.now(), 0, 0.2, "Normal"),
        ]
        
        for data in sample_data:
            try:
                cur.execute("""
                    INSERT INTO transactions 
                    (transaction_id, cc_number, user_id, amount, city, timestamp, predicted_fraud, fraud_score, fraud_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, data)
            except Exception as e:
                print(f"   Skipping {data[0]}: {e}")
        
        conn.commit()
        
        # Verify
        cur.execute("SELECT COUNT(*) FROM transactions")
        count = cur.fetchone()[0]
        print(f"✅ Database fixed. Total rows: {count}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database fix failed: {e}")
        return False

def run_quick_test():
    print("\n🔍 Quick Test of All Components...")
    
    # Test database
    db_ok = test_database()
    
    # Test API
    api_ok = test_fastapi()
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY:")
    print("="*60)
    
    if db_ok and api_ok:
        print("✅ All systems working!")
        print("👉 Now run: streamlit run dashboard.py")
    else:
        print("⚠️ Issues found:")
        if not db_ok:
            print("   - Database has issues")
            fix = input("   Fix database? (y/n): ")
            if fix.lower() == 'y':
                create_fix_database()
        if not api_ok:
            print("   - FastAPI not running")
            print("   Run: python fastapi_server.py")
    
    print("="*60)

if __name__ == "__main__":
    run_quick_test()