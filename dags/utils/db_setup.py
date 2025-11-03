# dags/utils/db_setup.py
"""Setup PostgreSQL database with sample data"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)

def get_db_engine():
    """Create PostgreSQL engine"""
    return create_engine('postgresql://airflow:airflow@postgres:5432/airflow')

def create_source_tables():
    """Create source tables in PostgreSQL"""
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # Drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS source_orders CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS source_customers CASCADE"))
        conn.commit()
        
        # Create orders table
        conn.execute(text("""
            CREATE TABLE source_orders (
                order_id VARCHAR(20) PRIMARY KEY,
                order_date TIMESTAMP NOT NULL,
                customer_id VARCHAR(20) NOT NULL,
                category VARCHAR(50),
                product VARCHAR(100),
                price DECIMAL(10, 2),
                quantity INTEGER,
                total DECIMAL(10, 2),
                status VARCHAR(20),
                payment_method VARCHAR(50),
                region VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Create customers table
        conn.execute(text("""
            CREATE TABLE source_customers (
                customer_id VARCHAR(20) PRIMARY KEY,
                customer_name VARCHAR(100),
                email VARCHAR(100),
                registration_date DATE,
                customer_segment VARCHAR(20),
                lifetime_value DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Create index for incremental loads
        conn.execute(text("""
            CREATE INDEX idx_orders_order_date ON source_orders(order_date)
        """))
        conn.execute(text("""
            CREATE INDEX idx_orders_updated_at ON source_orders(updated_at)
        """))
        
        conn.commit()
        logger.info("✅ Source tables created successfully")

def generate_historical_orders(start_date, end_date, orders_per_day=100):
    """Generate historical orders data"""
    np.random.seed(42)
    random.seed(42)
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys']
    products = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Camera', 'Smart Watch'],
        'Clothing': ['Shirt', 'Pants', 'Dress', 'Shoes', 'Jacket', 'Hat'],
        'Books': ['Fiction', 'Non-Fiction', 'Textbook', 'Magazine', 'Comic', 'Biography'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Lighting', 'Storage'],
        'Sports': ['Equipment', 'Apparel', 'Shoes', 'Accessories', 'Fitness', 'Outdoor'],
        'Toys': ['Action Figure', 'Puzzle', 'Board Game', 'Doll', 'Building Blocks', 'RC Car']
    }
    
    statuses = ['completed', 'pending', 'cancelled', 'returned']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash', 'bank_transfer']
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    data = []
    order_counter = 1
    
    current_date = start_date
    while current_date <= end_date:
        # Vary orders per day (simulate business fluctuation)
        daily_orders = random.randint(
            int(orders_per_day * 0.7), 
            int(orders_per_day * 1.3)
        )
        
        for _ in range(daily_orders):
            category = random.choice(categories)
            product = random.choice(products[category])
            
            # Generate order with some data quality issues
            order_id = f"ORD{order_counter:07d}"
            
            # Random time during the day
            order_time = current_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            customer_id = f"CUST{random.randint(1, 500):04d}"
            
            # Pricing logic
            base_prices = {
                'Electronics': (200, 2000),
                'Clothing': (20, 200),
                'Books': (10, 50),
                'Home': (50, 500),
                'Sports': (30, 300),
                'Toys': (15, 100)
            }
            price_range = base_prices[category]
            price = round(random.uniform(*price_range), 2)
            
            # Intentional data quality issues (2% of data)
            if random.random() < 0.02:
                price = -abs(price)  # Negative price
            
            quantity = random.randint(1, 10)
            if random.random() < 0.01:  # 1% invalid quantities
                quantity = 0
            
            status = random.choice(statuses)
            if random.random() < 0.01:  # 1% missing status
                status = ''
            
            # Calculate total with occasional errors
            total = round(price * quantity, 2)
            if random.random() < 0.03:  # 3% calculation errors
                total = round(total * random.uniform(0.8, 1.2), 2)
            
            data.append({
                'order_id': order_id,
                'order_date': order_time,
                'customer_id': customer_id,
                'category': category,
                'product': product,
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
        
        current_date += timedelta(days=1)
    
    return pd.DataFrame(data)

def generate_customers(n_customers=500):
    """Generate customer data"""
    np.random.seed(42)
    
    segments = ['Premium', 'Gold', 'Silver', 'Bronze']
    
    data = []
    for i in range(1, n_customers + 1):
        reg_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))
        
        data.append({
            'customer_id': f"CUST{i:04d}",
            'customer_name': f"Customer {i}",
            'email': f"customer{i}@example.com",
            'registration_date': reg_date.date(),
            'customer_segment': random.choice(segments),
            'lifetime_value': round(random.uniform(100, 50000), 2),
            'created_at': reg_date
        })
    
    return pd.DataFrame(data)

def populate_source_data():
    """Populate PostgreSQL with historical data"""
    engine = get_db_engine()
    
    # Generate and insert customers
    logger.info("📊 Generating customer data...")
    customers_df = generate_customers(500)
    customers_df.to_sql('source_customers', engine, if_exists='append', index=False)
    logger.info(f"✅ Inserted {len(customers_df)} customers")
    
    # Generate and insert orders (last 1 month)
    logger.info("📊 Generating orders data...")
    end_date = datetime(2025, 1, 31, 23, 59, 59)
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    
    orders_df = generate_historical_orders(
        start_date=start_date,
        end_date=end_date,
        orders_per_day=150  # ~150 orders per day
    )
    
    # Insert in chunks for better performance
    chunk_size = 1000
    for i in range(0, len(orders_df), chunk_size):
        chunk = orders_df.iloc[i:i+chunk_size]
        chunk.to_sql('source_orders', engine, if_exists='append', index=False)
        logger.info(f"📦 Inserted chunk {i//chunk_size + 1}: {len(chunk)} orders")
    
    logger.info(f"✅ Total orders inserted: {len(orders_df)}")
    
    # Print summary
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                DATE(order_date) as order_day,
                COUNT(*) as order_count,
                SUM(total) as daily_revenue
            FROM source_orders
            GROUP BY DATE(order_date)
            ORDER BY order_day
        """))
        
        logger.info("\n📈 Daily Summary:")
        for row in result:
            logger.info(f"  {row[0]}: {row[1]} orders, ${row[2]:,.2f} revenue")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("🚀 Setting up source database...")
    
    create_source_tables()
    populate_source_data()
    
    logger.info("✅ Database setup complete!")