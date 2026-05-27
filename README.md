This project is a Real-Time Retail Data Pipeline developed using Python, Apache Kafka, PySpark, and Hive to simulate how modern retail companies process large-scale streaming transaction data. 
The pipeline starts with a retail CSV dataset containing customer orders, product values, freight charges, review scores, and customer locations. 
A Kafka Producer streams the data row-by-row into a Kafka topic in real time, while a Kafka Consumer continuously consumes the records and stores them into a JSON file. 
PySpark is then used to process the streaming data, create Spark DataFrames, clean null values, perform exploratory data analysis (EDA), and generate business insights such as average sales, freight analysis, order status distribution, delivery time analysis, review score analysis, and city-wise sales performance. 
The processed data is finally stored using Hive integration and Parquet format for big data warehousing. This project demonstrates real-time streaming, ETL pipeline development, distributed data processing, analytics engineering, and enterprise-level big data workflow implementation.
                +-------------------+
                |   Retail CSV Data |
                +-------------------+
                           |
                           v
                +-------------------+
                | Kafka Producer    |
                | (Python Producer) |
                +-------------------+
                           |
                           v
                +-------------------+
                | Kafka Topic       |
                | target-orders     |
                +-------------------+
                           |
                           v
                +-------------------+
                | Kafka Consumer    |
                | (Python Consumer) |
                +-------------------+
                           |
                           v
                +-------------------+
                | JSON File Storage |
                | kafka_orders.json |
                +-------------------+
                           |
                           v
                +-------------------+
                | PySpark Processing|
                | Spark DataFrame   |
                +-------------------+
                           |
                           v
                +-------------------+
                | EDA & Analytics   |
                | Sales, Delivery,  |
                | Review Analysis   |
                +-------------------+
                           |
                           v
                +-------------------+
                | Hive Integration  |
                | retail_orders     |
                +-------------------+
                           |
                           v
                +-------------------+
                | Parquet Output    |
                | Processed Storage |
                +-------------------+

                ## Technologies Used

- Python
- Apache Kafka
- PySpark
- Apache Hive
- Pandas
- VS Code
