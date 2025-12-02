# scripts/insert_test_orders_postgres.py (FIXED)
"""
Insert test order data into PostgreSQL to verify Flink alert detection job.
"""

import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "ecommerce",
    "user": "app_user",
    "password": "app_password"
}

def main():
    print("🔌 Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Use transaction
        cur = conn.cursor()
        
        print("✅ Connected!")
        
        # Check table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'orders'
            );
        """)
        
        if not cur.fetchone()[0]:
            print("❌ Table 'orders' does not exist!")
            return
        
        print("✅ Table 'orders' exists")
        
        # Count current orders
        cur.execute("SELECT COUNT(*) FROM orders;")
        count_before = cur.fetchone()[0]
        print(f"\n📊 Current orders: {count_before}")
        
        # ================================================================
        # TEST ORDERS WITH SHORT IDs (MAX 20 CHARS)
        # ================================================================
        
        print("\n📝 Inserting test orders...")
        
        test_orders = [
            # ✅ NORMAL ORDERS
            {
                'order_id': 'TSTTT_NORM_001',  # ← 13 chars (OK!)
                'order_date': datetime.now(),
                'customer_id': 'CUST0001',
                'product_id': 'PROD0001',
                'category': 'Electronics',
                'product_name': 'Wireless Mouse',
                'price': 250.00,
                'quantity': 2,
                'total': 500.00,
                'status': 'completed',
                'payment_method': 'credit_card',
                'region': 'North'
            },
            {
                'order_id': 'TSTTT_NORM_002',
                'order_date': datetime.now(),
                'customer_id': 'CUST0002',
                'product_id': 'PROD0002',
                'category': 'Office',
                'product_name': 'Keyboard',
                'price': 1500.00,
                'quantity': 1,
                'total': 1500.00,
                'status': 'completed',
                'payment_method': 'debit_card',
                'region': 'South'
            },
            
            # 🚨 HIGH VALUE ALERT (total >= 50000)
            {
                'order_id': 'TSTTT_HI_001',  # ← Short!
                'order_date': datetime.now(),
                'customer_id': 'CUST0003',
                'product_id': 'PROD0003',
                'category': 'Electronics',
                'product_name': 'MacBook Pro M3',
                'price': 55000.00,
                'quantity': 1,
                'total': 55000.00,
                'status': 'completed',
                'payment_method': 'credit_card',
                'region': 'North'
            },
            {
                'order_id': 'TSTTT_HI_002',
                'order_date': datetime.now(),
                'customer_id': 'CUST0004',
                'product_id': 'PROD0004',
                'category': 'Electronics',
                'product_name': 'iPhone 15 Pro Max',
                'price': 30000.00,
                'quantity': 2,
                'total': 60000.00,
                'status': 'completed',
                'payment_method': 'bank_transfer',
                'region': 'Central'
            },
            {
                'order_id': 'TSTTT_HI_003',
                'order_date': datetime.now(),
                'customer_id': 'CUST0005',
                'product_id': 'PROD0005',
                'category': 'Electronics',
                'product_name': 'Gaming PC',
                'price': 75000.00,
                'quantity': 1,
                'total': 75000.00,
                'status': 'processing',
                'payment_method': 'credit_card',
                'region': 'East'
            },
            
            # 🚨 LARGE QUANTITY (quantity > 50)
            {
                'order_id': 'TSTTT_QTY_001',
                'order_date': datetime.now(),
                'customer_id': 'CUST0006',
                'product_id': 'PROD0006',
                'category': 'Office',
                'product_name': 'Pen',
                'price': 50.00,
                'quantity': 100,
                'total': 5000.00,
                'status': 'completed',
                'payment_method': 'credit_card',
                'region': 'East'
            },
            {
                'order_id': 'TSTTT_QTY_002',
                'order_date': datetime.now(),
                'customer_id': 'CUST0007',
                'product_id': 'PROD0007',
                'category': 'Office',
                'product_name': 'Notebook',
                'price': 25.00,
                'quantity': 200,
                'total': 5000.00,
                'status': 'completed',
                'payment_method': 'debit_card',
                'region': 'West'
            },
            
            # 🚨 NEGATIVE PRICE
            {
                'order_id': 'TSTTT_NEG_001',
                'order_date': datetime.now(),
                'customer_id': 'CUST0008',
                'product_id': 'PROD0008',
                'category': 'Voucher',
                'product_name': 'Discount',
                'price': -500.00,
                'quantity': 1,
                'total': -500.00,
                'status': 'completed',
                'payment_method': 'credit_card',
                'region': 'West'
            },
            
            # 🚨 ZERO QUANTITY
            {
                'order_id': 'TSTTT_ZERO_001',
                'order_date': datetime.now(),
                'customer_id': 'CUST0009',
                'product_id': 'PROD0009',
                'category': 'Test',
                'product_name': 'Invalid',
                'price': 1000.00,
                'quantity': 0,
                'total': 0.00,
                'status': 'pending',
                'payment_method': 'cash',
                'region': 'North'
            },
        ]
        
        # Insert with error handling
        inserted = 0
        failed = 0
        skipped = 0
        
        for order in test_orders:
            try:
                cur.execute("""
                    INSERT INTO orders (
                        order_id, order_date, customer_id, product_id,
                        category, product_name, price, quantity, total,
                        status, payment_method, region,
                        created_at, updated_at
                    ) VALUES (
                        %(order_id)s, %(order_date)s, %(customer_id)s, %(product_id)s,
                        %(category)s, %(product_name)s, %(price)s, %(quantity)s, %(total)s,
                        %(status)s, %(payment_method)s, %(region)s,
                        NOW(), NOW()
                    )
                    ON CONFLICT (order_id) DO NOTHING;
                """, order)
                
                if cur.rowcount > 0:
                    inserted += 1
                    alert_type = ""
                    if order['total'] >= 50000:
                        alert_type = "🚨 HIGH VALUE"
                    elif order['quantity'] > 50:
                        alert_type = "🚨 LARGE QTY"
                    elif order['price'] < 0:
                        alert_type = "🚨 NEGATIVE"
                    elif order['quantity'] <= 0:
                        alert_type = "🚨 ZERO QTY"
                    
                    print(f"   ✅ {order['order_id']}: {order['product_name']} | ${order['total']} | qty={order['quantity']} {alert_type}")
                else:
                    skipped += 1
                    print(f"   ⏭️  {order['order_id']}: Already exists")
                    
            except Exception as e:
                failed += 1
                print(f"   ❌ {order['order_id']}: {e}")
                # Rollback this order and continue
                conn.rollback()
                conn.autocommit = False
        
        # Commit all successful inserts
        conn.commit()
        
        # ================================================================
        # VERIFY
        # ================================================================
        
        print("\n📊 Verifying...")
        
        cur.execute("SELECT COUNT(*) FROM orders;")
        count_after = cur.fetchone()[0]
        
        print(f"   Before: {count_before}")
        print(f"   After: {count_after}")
        print(f"   Inserted: {inserted}")
        print(f"   Failed: {failed}")
        print(f"   Skipped: {skipped}")
        
        # Show test orders
        print("\n🔍 Test orders in database:")
        cur.execute("""
            SELECT 
                order_id, 
                product_name, 
                total, 
                quantity, 
                status,
                CASE 
                    WHEN total >= 50000 THEN '🚨 HIGH'
                    WHEN quantity > 50 THEN '🚨 QTY'
                    WHEN price < 0 THEN '🚨 NEG'
                    WHEN quantity <= 0 THEN '🚨 ZERO'
                    ELSE '✅'
                END as alert_flag
            FROM orders
            WHERE order_id LIKE 'TST_%'
            ORDER BY order_id;
        """)
        
        rows = cur.fetchall()
        
        if rows:
            print(f"\n   Found {len(rows)} test orders:")
            for row in rows:
                order_id, product, total, qty, status, flag = row
                print(f"   {flag} {order_id}: {product} | ${total} | qty={qty} | {status}")
        else:
            print("   ⚠️  No test orders found!")
        
        # Close
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        print("✅ COMPLETED!")
        print("="*60)
        
        if inserted > 0:
            print("\n📌 Next steps:")
            print("   1. Refresh DBeaver (F5)")
            print("   2. Check CDC captured changes:")
            print("      docker logs debezium 2>&1 | grep -i 'TST_'")
            print("   3. Check Kafka topic:")
            print("      docker exec kafka kafka-console-consumer \\")
            print("        --bootstrap-server localhost:9092 \\")
            print("        --topic cdc.public.orders \\")
            print("        --from-beginning --max-messages 10")
            print("   4. Check Flink alerts:")
            print("      docker exec kafka kafka-console-consumer \\")
            print("        --bootstrap-server localhost:9092 \\")
            print("        --topic flink-alerts \\")
            print("        --from-beginning --max-messages 10")
        
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        if conn:
            conn.rollback()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()