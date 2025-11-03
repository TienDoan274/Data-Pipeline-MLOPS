# scripts/setup_source_db.py
"""
Standalone script to setup source PostgreSQL database
Run once: python scripts/setup_source_db.py
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
    # Use environment variables or defaults
    host = os.getenv('SOURCE_DB_HOST', 'localhost')
    port = os.getenv('SOURCE_DB_PORT', '5434')
    database = os.getenv('SOURCE_DB_NAME', 'ecommerce')
    user = os.getenv('SOURCE_DB_USER', 'app_user')
    password = os.getenv('SOURCE_DB_PASSWORD', 'app_password')
    
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    logger.info(f"Connecting to: {host}:{port}/{database}")
    
    return create_engine(connection_string, pool_pre_ping=True)


def create_tables(engine):
    """Create source tables"""
    logger.info("📋 Creating tables...")
    
    with engine.connect() as conn:
        # Drop existing
        conn.execute(text("DROP TABLE IF EXISTS orders CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS customers CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS products CASCADE"))
        conn.commit()
        
        # Customers
        conn.execute(text("""
            CREATE TABLE customers (
                customer_id VARCHAR(20) PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                phone VARCHAR(20),
                registration_date DATE NOT NULL,
                customer_segment VARCHAR(20),
                lifetime_value DECIMAL(10, 2) DEFAULT 0,
                city VARCHAR(50),
                country VARCHAR(50) DEFAULT 'Vietnam',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Products
        conn.execute(text("""
            CREATE TABLE products (
                product_id VARCHAR(20) PRIMARY KEY,
                product_name VARCHAR(200) NOT NULL,
                category VARCHAR(50) NOT NULL,
                base_price DECIMAL(10, 2) NOT NULL,
                stock_quantity INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Orders
        conn.execute(text("""
            CREATE TABLE orders (
                order_id VARCHAR(20) PRIMARY KEY,
                order_date TIMESTAMP NOT NULL,
                customer_id VARCHAR(20) NOT NULL,
                product_id VARCHAR(20),
                category VARCHAR(50),
                product_name VARCHAR(200),
                price DECIMAL(10, 2) NOT NULL,
                quantity INTEGER NOT NULL,
                total DECIMAL(10, 2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                payment_method VARCHAR(50),
                region VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Indexes
        conn.execute(text("CREATE INDEX idx_orders_date ON orders(order_date)"))
        conn.execute(text("CREATE INDEX idx_orders_customer ON orders(customer_id)"))
        conn.execute(text("CREATE INDEX idx_orders_updated ON orders(updated_at)"))
        
        conn.commit()
    
    logger.info("✅ Tables created")


def generate_customers(n=500):
    """Generate customer data"""
    logger.info(f"👥 Generating {n} customers...")
    
    np.random.seed(42)
    segments = ['Premium', 'Gold', 'Silver', 'Bronze']
    cities = ['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Can Tho', 'Hai Phong']
    
    data = []
    for i in range(1, n + 1):
        reg_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))
        data.append({
            'customer_id': f"CUST{i:04d}",
            'customer_name': f"Customer {i}",
            'email': f"customer{i}@example.com",
            'phone': f"09{random.randint(10000000, 99999999)}",
            'registration_date': reg_date.date(),
            'customer_segment': random.choice(segments),
            'lifetime_value': round(random.uniform(100, 50000), 2),
            'city': random.choice(cities),
            'created_at': reg_date
        })
    
    return pd.DataFrame(data)


def generate_products(n=100):
    """Generate product catalog"""
    logger.info(f"📦 Generating {n} products...")
    
    np.random.seed(42)
    categories = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['Shirt', 'Pants', 'Dress', 'Shoes', 'Jacket'],
        'Books': ['Fiction', 'Non-Fiction', 'Textbook', 'Magazine'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding'],
        'Sports': ['Equipment', 'Apparel', 'Shoes', 'Accessories'],
        'Toys': ['Action Figure', 'Puzzle', 'Board Game', 'Doll']
    }
    
    base_prices = {
        'Electronics': (200, 2000),
        'Clothing': (20, 200),
        'Books': (10, 50),
        'Home': (50, 500),
        'Sports': (30, 300),
        'Toys': (15, 100)
    }
    
    data = []
    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        product_type = random.choice(categories[category])
        price_range = base_prices[category]
        
        data.append({
            'product_id': f"PROD{i:04d}",
            'product_name': f"{product_type} {i}",
            'category': category,
            'base_price': round(random.uniform(*price_range), 2),
            'stock_quantity': random.randint(0, 1000)
        })
    
    return pd.DataFrame(data)


def generate_orders(start_date, end_date, orders_per_day=150):
    """Generate orders for date range"""
    logger.info(f"🛒 Generating orders from {start_date.date()} to {end_date.date()}...")
    
    np.random.seed(42)
    random.seed(42)
    
    statuses = ['completed', 'pending', 'processing', 'cancelled', 'returned']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash', 'bank_transfer']
    regions = ['North', 'South', 'Central', 'East', 'West']
    
    customer_ids = [f"CUST{i:04d}" for i in range(1, 501)]
    product_ids = [f"PROD{i:04d}" for i in range(1, 101)]
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys']
    
    data = []
    order_counter = 1
    current_date = start_date
    
    while current_date <= end_date:
        daily_orders = random.randint(
            int(orders_per_day * 0.7),
            int(orders_per_day * 1.3)
        )
        
        for _ in range(daily_orders):
            order_time = current_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # Intentional data quality issues
            price = round(random.uniform(10, 2000), 2)
            if random.random() < 0.02:  # 2% negative prices (error)
                price = -abs(price)
            
            quantity = random.randint(1, 10)
            if random.random() < 0.01:  # 1% zero quantity (error)
                quantity = 0
            
            status = random.choice(statuses)
            if random.random() < 0.01:  # 1% missing status (error)
                status = ''
            
            total = round(price * quantity, 2)
            if random.random() < 0.03:  # 3% calculation errors
                total = round(total * random.uniform(0.8, 1.2), 2)
            
            data.append({
                'order_id': f"ORD{order_counter:07d}",
                'order_date': order_time,
                'customer_id': random.choice(customer_ids),
                'product_id': random.choice(product_ids),
                'category': random.choice(categories),
                'product_name': f"Product {random.randint(1, 100)}",
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
    
    logger.info(f"✅ Generated {len(data)} orders")
    return pd.DataFrame(data)


def insert_data(engine):
    """Insert all data"""
    # Customers
    customers_df = generate_customers(500)
    customers_df.to_sql('customers', engine, if_exists='append', index=False)
    logger.info(f"✅ Inserted {len(customers_df)} customers")
    
    # Products
    products_df = generate_products(100)
    products_df.to_sql('products', engine, if_exists='append', index=False)
    logger.info(f"✅ Inserted {len(products_df)} products")
    
    # Orders (1 month of data)
    start_date = datetime(2025, 10, 27, 0, 0, 0)
    end_date = datetime(2025, 11, 27, 23, 59, 59)
    orders_df = generate_orders(start_date, end_date, orders_per_day=150)
    
    # Insert in chunks for performance
    chunk_size = 1000
    total_inserted = 0
    for i in range(0, len(orders_df), chunk_size):
        chunk = orders_df.iloc[i:i+chunk_size]
        chunk.to_sql('orders', engine, if_exists='append', index=False)
        total_inserted += len(chunk)
        logger.info(f"  Inserted {total_inserted}/{len(orders_df)} orders...")
    
    logger.info(f"✅ Total orders inserted: {len(orders_df)}")


def show_summary(engine):
    """Show data summary"""
    logger.info("\n" + "="*60)
    logger.info("📊 DATABASE SUMMARY")
    logger.info("="*60)
    
    with engine.connect() as conn:
        # Table counts
        for table in ['customers', 'products', 'orders']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            logger.info(f"  {table.upper()}: {count:,} records")
        
        # Daily order summary
        logger.info("\n📈 Daily Order Summary:")
        result = conn.execute(text("""
            SELECT 
                DATE(order_date) as date,
                COUNT(*) as orders,
                ROUND(SUM(total)::numeric, 2) as revenue
            FROM orders
            GROUP BY DATE(order_date)
            ORDER BY date
            LIMIT 10
        """))
        
        for row in result:
            logger.info(f"  {row[0]}: {row[1]:3d} orders | ${row[2]:>10,.2f}")
        
        # Data quality issues
        logger.info("\n⚠️  Data Quality Issues (intentional):")
        
        result = conn.execute(text("SELECT COUNT(*) FROM orders WHERE price < 0"))
        logger.info(f"  Negative prices: {result.scalar()}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM orders WHERE quantity = 0"))
        logger.info(f"  Zero quantities: {result.scalar()}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM orders WHERE status = ''"))
        logger.info(f"  Missing status: {result.scalar()}")
        
        logger.info("\n✅ These issues will be cleaned in the Silver layer!")


def main():
    """Main setup function"""
    logger.info("🚀 Starting source database setup...\n")
    
    try:
        # Connect
        engine = get_source_db_connection()
        logger.info("✅ Connected to database\n")
        
        # Create tables
        create_tables(engine)
        
        # Insert data
        insert_data(engine)
        
        # Show summary
        show_summary(engine)
        
        logger.info("\n" + "="*60)
        logger.info("✅ SOURCE DATABASE SETUP COMPLETE!")
        logger.info("="*60)
        logger.info("\nYou can now run the Airflow pipeline to ingest this data.")
        logger.info("Next step: docker-compose up -d")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise


if __name__ == '__main__':
    main()