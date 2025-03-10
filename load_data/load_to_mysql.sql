-- chạy dbt online
tạo target
dbt docs generate --profiles-dir ./ --project-dir brazillian_ecom \
online= ==
dbt docs serve --profiles-dir ./ --project-dir brazillian_ecom
docker cp dataset/brazilian-ecommerce/ de_mysql:/tmp/
docker cp load_data/mysql_schemas.sql de_mysql:/tmp/

-- login to mysql server as root
make to_mysql_root
SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';
SET GLOBAL LOCAL_INFILE=TRUE;
exit

-- run commands
make to_mysql
source /tmp/mysql_schemas.sql;
show tables;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_items_dataset.csv' INTO TABLE
olist_order_items_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_payments_dataset.csv' INTO TABLE
olist_order_payments_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY
'\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset FIELDS TERMINATED BY ',' LINES TERMINATED
BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_products_dataset.csv' INTO TABLE
olist_products_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/product_category_name_translation.csv' INTO TABLE
product_category_name_translation FIELDS TERMINATED BY ',' LINES TERMINATED
BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_reviews_dataset.csv' INTO TABLE
olist_order_reviews_dataset FIELDS TERMINATED BY ',' LINES TERMINATED
BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_reviews_dataset.csv' INTO TABLE
olist_order_reviews_dataset FIELDS TERMINATED BY ',' LINES TERMINATED
BY '\n' IGNORE 1 ROWS;
# check tables records
SELECT * FROM olist_order_items_dataset LIMIT 5;
SELECT * FROM olist_order_payments_dataset LIMIT 5;
SELECT * FROM olist_orders_dataset LIMIT 5;
SELECT * FROM olist_products_dataset LIMIT 5;
SELECT * FROM product_category_name_translation LIMIT 5;
SELECT * FROM olist_order_reviews_dataset LIMIT 5;