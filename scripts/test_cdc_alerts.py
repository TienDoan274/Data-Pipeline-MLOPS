# insert_test_orders_postgres.py
"""
Insert test order data into PostgreSQL to verify Flink alert detection job.
Includes both normal and abnormal cases.
"""

import psycopg2

# Kết nối tới PostgreSQL
DB_CONFIG = {
    "host": "localhost",      # Nếu chạy trong Docker, service name có thể là 'postgres'
    "port": 5434,
    "dbname": "ecommerce",       # thay bằng tên database của bạn
    "user": "app_user",      # user
    "password": "app_password"   # password
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Tạo bảng nếu chưa tồn tại
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders_source (
            order_id VARCHAR(50) PRIMARY KEY,
            total DOUBLE PRECISION,
            quantity INT,
            price DOUBLE PRECISION,
            customer_id VARCHAR(50),
            product_name VARCHAR(255)
        );
    """)

    test_orders = [
        # ✅ Normal orders
        ("O1001", 200, 2, 100, "C1", "Mouse"),
        ("O1002", 450, 3, 150, "C2", "Keyboard"),

        # 🚨 Abnormal: total > 10000
        ("O2001", 15000, 5, 3000, "C3", "Laptop"),

        # 🚨 Abnormal: quantity > 50
        ("O2002", 5000, 60, 83, "C4", "Pen"),

        # 🚨 Abnormal: price < 0
        ("O2003", -500, 5, -100, "C5", "Discount Voucher"),

        # 🚨 Abnormal: quantity <= 0
        ("O2004", 999, 0, 999, "C6", "Invalid Quantity Item"),
    ]

    cur.executemany("""
        INSERT INTO orders (order_id, total, quantity, price, customer_id, product_name,order_date,status)
        VALUES (%s, %s, %s, %s, %s, %s,NOW(),'returned')
        ON CONFLICT (order_id) DO NOTHING;
    """, test_orders)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Inserted test data into PostgreSQL successfully!")

if __name__ == "__main__":
    main()
