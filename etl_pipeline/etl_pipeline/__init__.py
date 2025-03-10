import os
from dagster import Definitions
from . import assets
from .assets.bronze_layer    import olist_order_items_dataset, olist_order_payments_dataset, olist_order_reviews_dataset, olist_orders_dataset, olist_products_dataset, product_category_name_translation
from .assets.silver_layer    import silver_olist_products, silver_payment_summary_per_order, silver_review_score, silver_order_items
from .assets.gold_layer      import gold_order_summary
from .assets.warehouse_layer import brazillian_ecom_summary
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager  import PostgreSQLIOManager

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
defs = Definitions(
    assets=[
        olist_order_items_dataset,
        olist_order_payments_dataset,
        olist_order_reviews_dataset,
        olist_orders_dataset,
        olist_products_dataset,
        product_category_name_translation,
        silver_olist_products,
        silver_payment_summary_per_order,
        silver_review_score, 
        silver_order_items,
        gold_order_summary,
        brazillian_ecom_summary
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager" : PostgreSQLIOManager(PSQL_CONFIG)
    }
)