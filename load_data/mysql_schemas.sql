-- Xóa bảng nếu đã tồn tại
DROP TABLE IF EXISTS product_category_name_translation;
DROP TABLE IF EXISTS olist_products_dataset;
DROP TABLE IF EXISTS olist_orders_dataset;
DROP TABLE IF EXISTS olist_order_items_dataset;
DROP TABLE IF EXISTS olist_order_payments_dataset;

-- Tạo bảng product_category_name_translation
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(64) PRIMARY KEY,
    product_category_name_english VARCHAR(64)
);

-- Tạo bảng olist_products_dataset
CREATE TABLE olist_products_dataset (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(64),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name)
);

-- Tạo bảng olist_orders_dataset
CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32),
    order_status VARCHAR(16),
    order_purchase_timestamp VARCHAR(32),
    order_approved_at VARCHAR(32),
    order_delivered_carrier_date VARCHAR(32),
    order_delivered_customer_date VARCHAR(32),
    order_estimated_delivery_date VARCHAR(32)
);

-- Tạo bảng olist_order_items_dataset
CREATE TABLE olist_order_items_dataset (
    order_id VARCHAR(32),
    order_item_id INT,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date VARCHAR(32),
    price FLOAT,
    freight_value FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id, product_id, seller_id),
    FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id),
    FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id)
);

-- Tạo bảng olist_order_payments_dataset
CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(32),
    payment_sequential INT,
    payment_type VARCHAR(16),
    payment_installments INT,
    payment_value FLOAT,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
);


-- Bảng mới: olist_order_reviews_dataset
DROP TABLE IF EXISTS olist_order_reviews_dataset;
CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(32),
    order_id VARCHAR(32),
    review_score INT,
    review_creation_date VARCHAR(32),
    review_answer_timestamp VARCHAR(32),
    PRIMARY KEY (review_id),
    FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
);
