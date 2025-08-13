from wsgiref.simple_server import make_server
import dramatiq
import falcon
import pymysql
import random
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results.backends import RedisBackend
from dramatiq.results import Results
import json
import os
from faker import Faker

fake = Faker()

redis_broker = RedisBroker(url=os.getenv(
    'REDIS_URL', 'redis://localhost:6379/0'))
dramatiq.set_broker(redis_broker)
redis_broker.add_middleware(Results(backend=RedisBackend()))


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
@dramatiq.actor(actor_name="seed_database", queue_name="default")
def seed_database(num_products: int = 10, num_invoices: int = 100):
    print(
        f"[seed_database] Starting seeding with {num_products} products and {num_invoices} invoices")
    seed_database_impl(num_products, num_invoices)
    print("[seed_database] Seeding completed successfully")


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "product_revenue"),
        cursorclass=pymysql.cursors.DictCursor,
    )


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


app.add_route('/init-db', InitDBResource())
app.add_route('/seed', SeedResource())
app.add_route('/reports/product-revenue', ProductRevenueResource())
app.add_route('/reports/state-sales', StateSalesResource())


if __name__ == '__main__':
    with make_server('', 8000, app) as httpd:
        print('Serving on port 8000...')

        httpd.serve_forever()
