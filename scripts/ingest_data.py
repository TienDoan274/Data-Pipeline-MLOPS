"""
Add 30 days of data starting from Nov 28, 2025
Run: python scripts/add_30_days_data.py
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine, text
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_source_db_connection():
    """Get connection to source database"""
    host = os.getenv('SOURCE_DB_HOST', 'localhost')
    port = os.getenv('SOURCE_DB_PORT', '5434')
    database = os.getenv('SOURCE_DB_NAME', 'ecommerce')
    user = os.getenv('SOURCE_DB_USER', 'app_user')
    password = os.getenv('SOURCE_DB_PASSWORD', 'app_password')
    
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    logger.info(f"Connecting to: {host}:{port}/{database}")
    
    return create_engine(connection_string, pool_pre_ping=True)


def get_next_order_id(engine):
    """Get the next order ID to continue from existing data"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT order_id FROM orders 
            ORDER BY order_id DESC 
            LIMIT 1
        """))
        last_id = result.scalar()
        
        if last_id:
            # Extract number from ORD0000001 format
            last_num = int(last_id.replace('ORD', ''))
            return last_num + 1
        else:
            return 1


def generate_orders_30_days(start_date, engine, orders_per_day=150):
    """
    Generate 30 days of orders starting from Nov 28, 2025
    Includes intentional data quality issues for alert testing
    """
    logger.info(f"🛒 Generating 30 days of orders from {start_date.date()}...")
    
    np.random.seed(int(datetime.now().timestamp()))
    random.seed(int(datetime.now().timestamp()))
    
    statuses = ['completed', 'pending', 'processing', 'cancelled', 'returned']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash', 'bank_transfer']
    regions = ['North', 'South', 'Central', 'East', 'West']
    
    # Get existing customers and products
    with engine.connect() as conn:
        customers = conn.execute(text("SELECT customer_id FROM customers")).fetchall()
        products = conn.execute(text("SELECT product_id, category, product_name, base_price FROM products")).fetchall()
    
    customer_ids = [c[0] for c in customers]
    product_list = [(p[0], p[1], p[2], float(p[3])) for p in products]
    
    if not customer_ids or not product_list:
        logger.error("❌ No customers or products found. Run setup_source_db.py first!")
        return None
    
    logger.info(f"  Found {len(customer_ids)} customers and {len(product_list)} products")
    
    data = []
    order_counter = get_next_order_id(engine)
    end_date = start_date + timedelta(days=29)  # 30 days total
    current_date = start_date
    
    # Track for special test orders
    test_orders_added = False
    
    while current_date <= end_date:
        daily_orders = random.randint(
            int(orders_per_day * 0.7),
            int(orders_per_day * 1.3)
        )
        
        # Add special test orders on first day
        if not test_orders_added and current_date.date() == start_date.date():
            logger.info(f"  📍 Adding test orders with alerts on {current_date.date()}")
            
            # HIGH VALUE orders
            for i in range(3):
                order_time = current_date + timedelta(hours=10, minutes=i*5)
                product = random.choice(product_list)
                price = random.uniform(50000, 100000)
                
                data.append({
                    'order_id': f"ALERT_HI_{order_counter:05d}",
                    'order_date': order_time,
                    'customer_id': random.choice(customer_ids),
                    'product_id': product[0],
                    'category': product[1],
                    'product_name': f"High Value {product[2]}",
                    'price': round(price, 2),
                    'quantity': 1,
                    'total': round(price, 2),
                    'status': 'completed',
                    'payment_method': 'bank_transfer',
                    'region': random.choice(regions),
                    'created_at': order_time,
                    'updated_at': order_time
                })
                order_counter += 1
            
            # SUSPICIOUS QUANTITY orders
            for i in range(3):
                order_time = current_date + timedelta(hours=11, minutes=i*5)
                product = random.choice(product_list)
                quantity = random.randint(100, 200)
                price = random.uniform(10, 50)
                
                data.append({
                    'order_id': f"ALERT_QTY_{order_counter:05d}",
                    'order_date': order_time,
                    'customer_id': random.choice(customer_ids),
                    'product_id': product[0],
                    'category': product[1],
                    'product_name': product[2],
                    'price': round(price, 2),
                    'quantity': quantity,
                    'total': round(price * quantity, 2),
                    'status': 'processing',
                    'payment_method': 'credit_card',
                    'region': random.choice(regions),
                    'created_at': order_time,
                    'updated_at': order_time
                })
                order_counter += 1
            
            # NEGATIVE PRICE orders
            for i in range(2):
                order_time = current_date + timedelta(hours=12, minutes=i*5)
                product = random.choice(product_list)
                
                data.append({
                    'order_id': f"ALERT_NEG_{order_counter:05d}",
                    'order_date': order_time,
                    'customer_id': random.choice(customer_ids),
                    'product_id': product[0],
                    'category': 'Refund',
                    'product_name': 'Refund/Discount',
                    'price': round(-random.uniform(100, 1000), 2),
                    'quantity': 1,
                    'total': round(-random.uniform(100, 1000), 2),
                    'status': 'completed',
                    'payment_method': 'refund',
                    'region': random.choice(regions),
                    'created_at': order_time,
                    'updated_at': order_time
                })
                order_counter += 1
            
            # INVALID QUANTITY orders
            for i in range(2):
                order_time = current_date + timedelta(hours=13, minutes=i*5)
                product = random.choice(product_list)
                
                data.append({
                    'order_id': f"ALERT_ZERO_{order_counter:05d}",
                    'order_date': order_time,
                    'customer_id': random.choice(customer_ids),
                    'product_id': product[0],
                    'category': product[1],
                    'product_name': product[2],
                    'price': round(random.uniform(100, 500), 2),
                    'quantity': 0,
                    'total': 0.0,
                    'status': 'pending',
                    'payment_method': 'credit_card',
                    'region': random.choice(regions),
                    'created_at': order_time,
                    'updated_at': order_time
                })
                order_counter += 1
            
            test_orders_added = True
            logger.info(f"  ✅ Added 10 test orders with alert conditions")
        
        # Regular orders for the day
        for _ in range(daily_orders):
            order_time = current_date + timedelta(
                hours=random.randint(8, 22),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            product = random.choice(product_list)
            
            # Mostly normal orders, with occasional issues
            price = round(random.uniform(product[3] * 0.8, product[3] * 1.2), 2)
            
            # 2% chance of negative price (refund/discount)
            if random.random() < 0.02:
                price = -abs(price)
            
            quantity = random.randint(1, 10)
            
            # 1% chance of zero quantity
            if random.random() < 0.01:
                quantity = 0
            
            # 0.5% chance of high quantity
            if random.random() < 0.005:
                quantity = random.randint(51, 100)
            
            total = round(price * quantity, 2)
            
            # 0.5% chance of high value order
            if random.random() < 0.005:
                price = round(random.uniform(10000, 50000), 2)
                quantity = 1
                total = price
            
            status = random.choice(statuses)
            
            data.append({
                'order_id': f"ORD{order_counter:07d}",
                'order_date': order_time,
                'customer_id': random.choice(customer_ids),
                'product_id': product[0],
                'category': product[1],
                'product_name': product[2],
                'price': price,
                'quantity': quantity,
                'total': total,
                'status': status,
                'payment_method': random.choice(payment_methods),
                'region': random.choice(regions),
                'created_at': order_time,
                'updated_at': order_time
            })
            
            order_counter += 1
        
        # Progress log every 5 days
        if (current_date - start_date).days % 5 == 0:
            logger.info(f"  Day {(current_date - start_date).days + 1}/30: {len(data)} orders generated so far...")
        
        current_date += timedelta(days=1)
    
    logger.info(f"✅ Generated {len(data)} orders for 30 days")
    return pd.DataFrame(data)


def insert_orders(engine, orders_df):
    """Insert orders in chunks"""
    chunk_size = 1000
    total_inserted = 0
    
    logger.info(f"📤 Inserting {len(orders_df)} orders in chunks of {chunk_size}...")
    
    for i in range(0, len(orders_df), chunk_size):
        chunk = orders_df.iloc[i:i+chunk_size]
        chunk.to_sql('orders', engine, if_exists='append', index=False)
        total_inserted += len(chunk)
        logger.info(f"  Progress: {total_inserted}/{len(orders_df)} orders inserted...")
    
    logger.info(f"✅ Total inserted: {len(orders_df)} orders")


def show_summary(engine, start_date):
    """Show summary of new data"""
    logger.info("\n" + "="*60)
    logger.info("📊 NEW DATA SUMMARY")
    logger.info("="*60)
    
    with engine.connect() as conn:
        # Total orders
        result = conn.execute(text("SELECT COUNT(*) FROM orders"))
        total = result.scalar()
        logger.info(f"  Total orders in database: {total:,}")
        
        # New orders count
        end_date = start_date + timedelta(days=29)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM orders 
            WHERE order_date >= :start AND order_date <= :end
        """), {"start": start_date, "end": end_date})
        new_count = result.scalar()
        logger.info(f"  New orders added: {new_count:,}")
        
        # Daily breakdown for last 7 days
        logger.info(f"\n📈 Last 7 Days Breakdown:")
        result = conn.execute(text("""
            SELECT 
                DATE(order_date) as date,
                COUNT(*) as orders,
                ROUND(SUM(total)::numeric, 2) as revenue
            FROM orders
            WHERE order_date >= :start
            GROUP BY DATE(order_date)
            ORDER BY date DESC
            LIMIT 7
        """), {"start": start_date})
        
        for row in result:
            logger.info(f"  {row[0]}: {row[1]:3d} orders | ${row[2]:>10,.2f}")
        
        # Alert conditions
        logger.info(f"\n🚨 Orders Matching Alert Conditions:")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM orders 
            WHERE order_date >= :start AND total > 10000
        """), {"start": start_date})
        logger.info(f"  High value (>$10,000): {result.scalar()}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM orders 
            WHERE order_date >= :start AND quantity > 50
        """), {"start": start_date})
        logger.info(f"  High quantity (>50): {result.scalar()}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM orders 
            WHERE order_date >= :start AND price < 0
        """), {"start": start_date})
        logger.info(f"  Negative price: {result.scalar()}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM orders 
            WHERE order_date >= :start AND quantity <= 0
        """), {"start": start_date})
        logger.info(f"  Zero/negative quantity: {result.scalar()}")


def main():
    """Main function"""
    logger.info("🚀 Adding 30 days of data starting Nov 28, 2025...\n")
    
    try:
        # Connect
        engine = get_source_db_connection()
        logger.info("✅ Connected to database\n")
        
        # Start date
        start_date = datetime(2025, 12, 2, 0, 0, 0)
        
        # Generate orders
        orders_df = generate_orders_30_days(start_date, engine, orders_per_day=150)
        
        if orders_df is None:
            return
        
        # Insert
        insert_orders(engine, orders_df)
        
        # Summary
        show_summary(engine, start_date)
        
        logger.info("\n" + "="*60)
        logger.info("✅ DATA ADDITION COMPLETE!")
        logger.info("="*60)
        logger.info("\n📌 Next Steps:")
        logger.info("1. Monitor CDC topic: docker exec kafka kafka-console-consumer \\")
        logger.info("     --bootstrap-server localhost:9092 \\")
        logger.info("     --topic cdc.public.orders --from-beginning")
        logger.info("\n2. Check Flink alerts: docker exec kafka kafka-console-consumer \\")
        logger.info("     --bootstrap-server localhost:9092 \\")
        logger.info("     --topic flink-alerts --from-beginning")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise


if __name__ == '__main__':
    main()