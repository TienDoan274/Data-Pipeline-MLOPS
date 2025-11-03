# dags/utils/data_generator.py
"""Generate sample e-commerce data"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_raw_orders(n_orders=1000):
    """Generate raw orders data (Bronze layer)"""
    np.random.seed(42)
    
    # Simulate date range
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(n_orders)]
    
    # Product categories
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys']
    products = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones'],
        'Clothing': ['Shirt', 'Pants', 'Dress', 'Shoes'],
        'Books': ['Fiction', 'Non-Fiction', 'Textbook', 'Magazine'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding'],
        'Sports': ['Equipment', 'Apparel', 'Shoes', 'Accessories'],
        'Toys': ['Action Figure', 'Puzzle', 'Board Game', 'Doll']
    }
    
    # Generate data with intentional quality issues
    data = []
    for i in range(n_orders):
        category = random.choice(categories)
        product = random.choice(products[category])
        
        # Intentional data quality issues:
        order_id = f"ORD{i:05d}" if random.random() > 0.02 else None  # 2% missing
        customer_id = f"CUST{random.randint(1, 200):04d}"
        
        # Some negative prices (data errors)
        price = round(random.uniform(10, 1000), 2)
        if random.random() < 0.01:  # 1% invalid prices
            price = -price
        
        # Some invalid quantities
        quantity = random.randint(1, 10)
        if random.random() < 0.01:  # 1% invalid quantities
            quantity = 0
        
        # Missing/invalid status
        statuses = ['completed', 'pending', 'cancelled', 'returned']
        status = random.choice(statuses) if random.random() > 0.01 else ''
        
        # Incorrect total calculations (data quality issue)
        total = round(price * quantity, 2)
        if random.random() < 0.05:  # 5% calculation errors
            total = total * random.uniform(0.5, 1.5)
        
        data.append({
            'order_id': order_id,
            'order_date': dates[i].strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': customer_id,
            'category': category,
            'product': product,
            'price': price,
            'quantity': quantity,
            'total': round(total, 2),
            'status': status,
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'cash']),
            'region': random.choice(['North', 'South', 'East', 'West', 'Central']),
        })
    
    return pd.DataFrame(data)

def generate_customer_data(n_customers=200):
    """Generate customer master data"""
    np.random.seed(42)
    
    data = []
    for i in range(1, n_customers + 1):
        data.append({
            'customer_id': f"CUST{i:04d}",
            'customer_name': f"Customer {i}",
            'email': f"customer{i}@example.com",
            'registration_date': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'lifetime_value': round(random.uniform(100, 10000), 2)
        })
    
    return pd.DataFrame(data)