Lớp sliver
asset dim_olist_products
thực hiện join hai bảng product và translate_englist nhằm lấy tên tiếng anh của đơn hàng
-> output asset này là olist_product_dataset thêm cột product_name_english

+ Nhận xét bảng order_payment: mỗi order_id có thể có nhiều lần thanh toán
   ---> solution: tính tổng lại payment_value gom nhóm theo order_id (tổng giá trị mỗi đơn hàng)
                  đếm số lượng payment_type cho mỗi order_id(tổng số lần thanh toán)
   --> output: tổng thanh toán, count(payment_type) cho mỗi order_id 

+ Bảng review:
    --> solution: tình tổng số lượng đánh giá, tính điểm đánh giá trung bình cho từng order_id, 
    -->  --> ouput bảng review_score(order_id, score_review_avg, total_review)

+ Bảng olist_order_items_dataset
   Mỗi đơn hàng sẽ có nhiều items khác nhau(item_id) --> tính tổng item cho mỗi order_id
   Join kết quả ở bảng trên(chỉ lấy cột count(item)ở trên) với bảng olist_order_dataset (on order_id)--> đây là bảng gốc
   các cột order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date trong bảng đó đang ở định dạng varchar--> chuyển về timestamp(bỏ thời gian, giữ ngày tháng năm)


   ++ Lớp gold
   join 4 bảng với nhau: bảng gốc là silver_order_items
    - join với silver_olist_products theo product_id
    - join với silver_payment_summary_per_order theo order_id
    - join với silver_review_score theo order_id

    ===> Sau khi kiểm tra bảng gold_summary_order phát hiện rất nhiều order_id không có review_score
   ==> đặt các ô trống là na
   ------------------------------
   dựa vào prompt nội dung readme dưới đây viết cho tôi hoàn chỉnh, bằng tiếng anh, và trình bày hay hơn, có logic hơn
   Hình ảnh etl_pipeline(hướng dẫn tôi làm sao để bỏ ảnh vào readme)
   (mở đầu viết lại bằng tiếng anh và viết hay hơn)
   This project implements a comprehensive ETL (Extract, Transform, Load) pipeline using dagster, minio, DBT for processing bộ dữ liệu brazillian_ecommerce. Thông qua toàn bộ quá trình up và load dữ liệu nhằm xây dựng ra bộ dataset cuôi scungf phục vụ cho mục đích DA

   Directory-tree(vẽ lại sơ đồ và định nghĩa từng file)
   Quy trình trong toàn bộ, bộ dữ liệu raw data ban đầu được đẩy vào mysql ở format csv , sau đó load vào datalake MINIo --> tại đây thực hiện 3 quy trình load dữ liệu qua 3 lớp: bronze-silver-gold
   đây là hình ảnh quy trình thwujc hiện tại minio
   ![alt text](image-2.png)
      + lớp bronze: dữ liệu thô được lấy từ mysql với 6 table olist_order_items_dataset,
        olist_order_payments_dataset,
        olist_order_reviews_dataset,
        olist_orders_dataset,
        olist_products_dataset,
        product_category_name_translation,
        (thêm hình ảnh)
      thêm hình ảnh 
      + lớp silver: tiến hành gom nhóm, dùng quy trình DA để phân tích và join các bảng 
      thêm hình ảnh 
      + lớp gold: join 4 bảng ở lớp silver thành 1 bộ dataset hoàn chỉnh
      + lớp warehouse: load data từ gold_layer sang postgres
      (thêm hình ảnh)

      Bộ dataset cuối cùng chứa thông tin của order theo các thông tin về sản phẩm, đánh giá, thời gian liên quan đơn hàng, thanh toán, hóa đơn... nhằm phục vụ cho mục đích phân tích kinh doanh
      Nội dung này sẽ được tôi triển khai ở một project DA sắp tới

      Mục tiêu tiếp theo: app dbt vào postgres để quá trình được như ....( điền thêm)
      với nguồn dữ liệu lơn, sử dụng conjob để lên lịch load dữ liệu
      các nền tảng đám mây như aws
(các dấu -> biểu thị thư mục đó vẫn chứa những mục con, hãy đổi thành kí tự khác phù hợp hơn) 
dagster->
dagster_home->
Dataset->
Dockerimages->
etl_pipeline
      dbt_ecommerce
      etl_pipeline
            assets
                  bronze_layer.py
                  silver_layer.py
                  warehouse_layer.py
            resources
                  minio_io_manager.py
                  mysql_io_manager.py
                  psql_io_manager.py
            __init__.py
      Dockerfile
      requirements.txt
      -->
docker-compose.yaml
env
Makefile
load_data->
README.md
   

