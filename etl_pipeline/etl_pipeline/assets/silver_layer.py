from dagster import asset, AssetIn, Output
import pandas as pd

COMPUTE_KIND = "Pandas"
LAYER = "silver"
SCHEMA = "ecommerce"

@asset(
    io_manager_key="minio_io_manager",
    ins={

        "olist_products_dataset": AssetIn(key_prefix=["bronze", SCHEMA]),
        "product_category_name_translation": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="translate product names into English"
)
def silver_olist_products(context,olist_products_dataset,product_category_name_translation) -> Output[pd.DataFrame]:

    # Sử dụng Pandas DataFrame để làm việc với dữ liệu
    olist_products = pd.DataFrame(olist_products_dataset)
    product_category_translation = pd.DataFrame(product_category_name_translation)
    
    # Thực hiện JOIN giữa hai DataFrame trên cột "product_category_name"
    df = pd.merge(
        olist_products, 
        product_category_translation, 
        on="product_category_name",  # Cột chung giữa hai bảng để join
        how="inner"  # Sử dụng INNER JOIN
    )
    
    # Chọn các cột cần thiết (product_id và product_category_name_english)
    df = df[["product_id", "product_category_name_english"]]
    
    # Loại bỏ các bản ghi trùng lặp
    df = df.drop_duplicates()
    
    # Trả về Output với metadata
    return Output(
        df,
        metadata={
            "table": "silver_olist_products",
            "records count": len(df),
        }
    )

## asset 2

@asset(
    ins={
        "olist_order_payments_dataset": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    description="Summarize total payment value and count total payment per order",
    key_prefix=[LAYER, SCHEMA],
    io_manager_key="minio_io_manager",
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def silver_payment_summary_per_order(context, olist_order_payments_dataset) -> Output[pd.DataFrame]:
    
    # Sử dụng Pandas DataFrame để làm việc với dữ liệu
    payments_df = pd.DataFrame(olist_order_payments_dataset)
    
    # Tính tổng payment_value và đếm số lượng payment_type cho mỗi order_id
    summary_df = payments_df.groupby('order_id').agg(
        total_payment_value=pd.NamedAgg(column='payment_value', aggfunc='sum'),  # Tính tổng payment_value
        payment_type_count=pd.NamedAgg(column='payment_type', aggfunc='count')    # Đếm số lần thanh toán
    ).reset_index()
    
    # Trả về Output với metadata
    return Output(
        summary_df,
        metadata={
            "table": "silver_payment_summary_per_order",
            "records count": len(summary_df),
        }
    )

## asset 3
@asset(
    io_manager_key="minio_io_manager",
    ins={

        "olist_order_reviews_dataset": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Summarize total review count and average review score per order"
)
def silver_review_score(context, olist_order_reviews_dataset) -> Output[pd.DataFrame]:
    reviews_df = pd.DataFrame(olist_order_reviews_dataset)
    # Tính tổng số lượng đánh giá và điểm đánh giá trung bình cho mỗi order_id
    reviews_df['review_score'] = reviews_df['review_score'].astype(float)
    summary_df = reviews_df.groupby('order_id').agg(
        score_review_avg=pd.NamedAgg(column='review_score', aggfunc='mean'),  # Tính điểm đánh giá trung bình
        total_review=pd.NamedAgg(column='review_id', aggfunc='count')         # Đếm số lượng đánh giá
    ).reset_index()

    summary_df['score_review_avg'] = summary_df['score_review_avg'].round(2)

    # Trả về Output với metadata
    return Output(
        summary_df,
        metadata={
            "table": "silver_review_score",
            "records count": len(summary_df),
        }
    )

# asset 4
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "olist_order_items_dataset": AssetIn(key_prefix=["bronze", SCHEMA]),
        "olist_orders_dataset": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Total items per order"
)
def silver_order_items(context, olist_order_items_dataset, olist_orders_dataset) -> Output[pd.DataFrame]:
    # Chuyển olist_order_items_dataset thành DataFrame
    items_df = pd.DataFrame(olist_order_items_dataset)
    
    # Tính tổng số lượng item cho mỗi order_id
    item_count_df = items_df.groupby(['order_id','product_id']).agg(
        total_items=pd.NamedAgg(column='order_item_id', aggfunc='max')
    ).reset_index()

    # Chuyển olist_orders_dataset thành DataFrame
    orders_df = pd.DataFrame(olist_orders_dataset)

    # Chuyển đổi các cột ngày tháng từ varchar sang timestamp (chỉ giữ ngày)
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'], format='%Y-%m-%d').dt.date
    orders_df['order_delivered_customer_date'] = pd.to_datetime(orders_df['order_delivered_customer_date'], format='%Y-%m-%d').dt.date
    orders_df['order_estimated_delivery_date'] = pd.to_datetime(orders_df['order_estimated_delivery_date'], format='%Y-%m-%d').dt.date

    # Join kết quả từ item_count_df với orders_df dựa trên order_id
    result_df = pd.merge(orders_df, item_count_df, on='order_id', how='left')
    result_df = result_df[['order_id','total_items','product_id','customer_id','order_status', 'order_purchase_timestamp','order_estimated_delivery_date','order_delivered_customer_date']]

    # Trả về Output với metadata
    return Output(
        result_df,
        metadata={
            "table": "silver_order_items",
            "records count": len(result_df),
        }
    )
