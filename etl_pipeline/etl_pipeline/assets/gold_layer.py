from dagster import asset, AssetIn, Output
import pandas as pd

COMPUTE_KIND = "Pandas"
LAYER = "gold"
SCHEMA = "ecommerce"

# pickup_location_lat_lon
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_olist_products": AssetIn(key_prefix=["silver",SCHEMA]),
        "silver_payment_summary_per_order": AssetIn(key_prefix=["silver",SCHEMA]),
        "silver_review_score": AssetIn(key_prefix=["silver",SCHEMA]),
        "silver_order_items": AssetIn(key_prefix=["silver",SCHEMA])
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="merge and build summary dataset"
)
def gold_order_summary(context, silver_order_items, silver_olist_products, silver_payment_summary_per_order, silver_review_score) -> Output[pd.DataFrame]:
    
    order_items_df = pd.DataFrame(silver_order_items)
    products_df = pd.DataFrame(silver_olist_products)
    payment_df = pd.DataFrame(silver_payment_summary_per_order)
    review_df = pd.DataFrame(silver_review_score)
    
    # Join dữ liệu từ các bảng
    merged_df = order_items_df \
        .merge(products_df, on="product_id", how="left") \
        .merge(payment_df, on="order_id", how="left") \
        .merge(review_df, on="order_id", how="left")
    
    # Phát hiện rất nhiều order_id không được review
    # merged_df['total_review'] = merged_df['total_review'].fillna(-1)
    # merged_df['score_review_avg'] = merged_df['score_review_avg'].fillna(-1)
    
        # Xác định cột số và cột chuỗi
    # Xác định cột số, cột ngày giờ, và cột chuỗi
    num_cols = ['total_review', 'score_review_avg', 'total_items', 'total_payment_value', 'payment_type_count']
    date_cols = ['order_purchase_timestamp', 'order_estimated_delivery_date', 'order_delivered_customer_date']
    str_cols = ['order_id', 'product_id', 'customer_id', 'order_status', 'product_category_name_english']
    
    
    # Điền giá trị NaN trong cột số bằng -1 (hoặc 0 nếu phù hợp)
    for col in num_cols:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(-1)
            
    for col in date_cols:
        if col in merged_df.columns:
            merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce')  # Chuyển về datetime
            merged_df[col] = merged_df[col].fillna(pd.NaT)  # Điền giá trị NaN bằng NaT

    # Điền giá trị NaN trong cột chuỗi bằng "NA"
    for col in str_cols:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna("NA")
        # Danh sách các cột cho bảng
    # my_table = ['order_id', 'total_items', 'product_id', 'customer_id', 'order_status', 
    #             'order_purchase_timestamp', 'order_estimated_delivery_date', 
    #             'order_delivered_customer_date', 'product_category_name_english', 
    #             'total_payment_value', 'payment_type_count']
    
    # # Lấp các ô trống trong tất cả các cột của my_table bằng 'NA'
    # merged_df[my_table] = merged_df[my_table].fillna('-1')
    # Trả về Output với metadata
    return Output(
        merged_df,
        metadata={
            "table": "gold_order_summary",
            "records count": len(merged_df),
        }
    )