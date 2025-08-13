from wsgiref.simple_server import make_server
import dramatiq
import falcon
import pymysql
import random
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results.backends import RedisBackend
from dramatiq.results import Results
import json
import os
from faker import Faker

fake = Faker()


def create_tables():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS Products (
                    ProductID INT PRIMARY KEY AUTO_INCREMENT,
                    ProductName VARCHAR(100) NOT NULL,
                    Category VARCHAR(50) NULL,
                    UnitPrice DECIMAL(10,2) NOT NULL,
                    State VARCHAR(50) NOT NULL
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS Invoices (
                    InvoiceID INT PRIMARY KEY AUTO_INCREMENT,
                    ProductID INT,
                    Quantity INT NOT NULL,
                    InvoiceDate DATE NOT NULL,
                    CustomerState VARCHAR(50) NOT NULL,
                    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
                        ON DELETE CASCADE
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


def seed_database_impl(num_products: int = 10, num_invoices: int = 100):
    create_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            product_rows = []
            for _ in range(num_products):
                product_rows.append(
                    (
                        fake.word().title(),
                        fake.random_element(
                            ['Electronics', 'Home', 'Fashion', 'Grocery', 'Sports']),
                        round(random.uniform(5.0, 500.0), 2),
                        fake.state()
                    )
                )

            cursor.executemany(
                """INSERT INTO Products (ProductName, Category, UnitPrice, State)
                   VALUES (%s, %s, %s, %s)""",
                product_rows,
            )

            cursor.execute("SELECT ProductID, UnitPrice FROM Products")
            products = cursor.fetchall()

            invoice_rows = []
            for _ in range(num_invoices):
                product = random.choice(products)
                invoice_rows.append(
                    (
                        product["ProductID"],
                        random.randint(1, 10),
                        fake.date_between(start_date="-1y", end_date="today"),
                        fake.state(),
                    )
                )

            cursor.executemany(
                """INSERT INTO Invoices (ProductID, Quantity, InvoiceDate, CustomerState)
                   VALUES (%s, %s, %s, %s)""",
                invoice_rows,
            )
        conn.commit()
    finally:
        conn.close()


# actor to seed DB in background
@dramatiq.actor
def seed_database(num_products: int = 10, num_invoices: int = 100):
    seed_database_impl(num_products, num_invoices)


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "product_revenue"),
        cursorclass=pymysql.cursors.DictCursor,
    )


# def fake_product_data(num_product=10):
#     products = []
#     for _ in range(num_product):
#         product = {
#             'product_id': fake.uuid4(),
#             'invoice_id': fake.uuid4(),
#             'invoice_date': fake.date_time_this_year(),
#             'customer_state': fake.state(),
#             'quantity_sold': fake.random_number(digits=3)
#         }
#         products.append(product)
#     return products


# def fake_sale_data(num_orders=100, product_data=None):
#     if product_data is None:
#         product_data = fake_product_data(num_product=10)

#     orders = []
#     product_ids = [product['product_id'] for product in product_data]
#     for _ in range(num_orders):
#         order_date = fake.date_between(start_date='-1y', end_date='today')
#         items_in_order = []
#         for _ in range(fake.random.randint(1, 5)):
#             product_id = random.choice(product_ids)
#             quantity = fake.random.randint(1, 10)
#             items_in_order.append({
#                 'product_id': product_id,
#                 'quantity': quantity
#             })
#         order = {
#             'order_id': fake.unique.uuid4(),
#             'customer_id': fake.unique.uuid4(),
#             'order_date': order_date.strftime('%Y-%m-%d'),
#             'total_amount': round(random.uniform(10.0, 1000.0), 2),
#             'items': items_in_order,
#             'status': fake.random.choice(['pending', 'shipped', 'delivered', 'cancelled'])
#         }
#         orders.append(order)
#     return orders


def get_product_revenue_report():
    create_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    p.ProductID,
                    p.ProductName,
                    SUM(i.Quantity * p.UnitPrice) AS TotalRevenue
                FROM Products p
                JOIN Invoices i ON i.ProductID = p.ProductID
                GROUP BY p.ProductID, p.ProductName
                ORDER BY TotalRevenue DESC;
                """
            )
            rows = cursor.fetchall()
            for r in rows:
                r["TotalRevenue"] = float(r["TotalRevenue"] or 0)
            return rows
    finally:
        conn.close()


def get_state_sales_report():
    create_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.CustomerState AS State,
                    SUM(i.Quantity * p.UnitPrice) AS TotalRevenue
                FROM Invoices i
                JOIN Products p ON p.ProductID = i.ProductID
                GROUP BY i.CustomerState
                ORDER BY TotalRevenue DESC;
                """
            )
            rows = cursor.fetchall()
            for r in rows:
                r["TotalRevenue"] = float(r["TotalRevenue"] or 0)
            return rows
    finally:
        conn.close()


class InitDBResource:
    def on_post(self, req, resp):
        create_tables()
        resp.status = falcon.HTTP_200
        resp.media = {"status": "ok", "message": "Tables are ready."}


class SeedResource:
    def on_post(self, req, resp):
        num_products = int(req.get_param("num_products") or 10)
        num_invoices = int(req.get_param("num_invoices") or 100)
        seed_database.send(num_products, num_invoices)
        resp.status = falcon.HTTP_202
        resp.media = {"status": "accepted", "message": "seeding database"}


class SeedSyncResource:
    def on_post(self, req, resp):
        num_products = int(req.get_param("num_products") or 10)
        num_invoices = int(req.get_param("num_invoices") or 100)
        try:
            seed_database_impl(num_products, num_invoices)
            resp.status = falcon.HTTP_200
            resp.media = {"status": "ok",
                          "message": "Database seeded successfully"}
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.media = {"status": "error", "message": str(e)}


class ProductRevenueResource:
    def on_get(self, req, resp):
        resp.media = {"data": get_product_revenue_report()}
        resp.status = falcon.HTTP_200


class StateSalesResource:
    def on_get(self, req, resp):
        resp.media = {"data": get_state_sales_report()}
        resp.status = falcon.HTTP_200


class ThingsResource:
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_TEXT
        resp.text = (
            '\nTwo things awe me most, the starry sky '
            'above me and the moral law within me.\n'
            '\n'
            '    ~ Immanuel Kant\n\n'
        )


app = falcon.App()

things = ThingsResource()


app.add_route('/things', things)

rabbitmq_broker = RabbitmqBroker(url=os.getenv(
    'RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/'))
dramatiq.set_broker(rabbitmq_broker)


app.add_route('/init-db', InitDBResource())
app.add_route('/seed', SeedResource())
app.add_route('/seed-sync', SeedSyncResource())
app.add_route('/reports/product-revenue', ProductRevenueResource())
app.add_route('/reports/state-sales', StateSalesResource())


if __name__ == '__main__':
    with make_server('', 8000, app) as httpd:
        print('Serving on port 8000...')

        httpd.serve_forever()
