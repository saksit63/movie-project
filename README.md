# End-to-End Movie ETL Pipeline

Technology used: *Python, PySpark, Apache Airflow, Docker,Google Cloud Dataproc, Google Cloud Storage, Google Cloud Bigquery, Google Looker Studio*

![Data Pipeline Diagram](https://github.com/saksit63/movie-project/blob/main/img/movie_workflow.png)

## Process
 1. นำข้อมูลการแสดงความคิดของลูกค้าจากไฟล์ CSV และข้อมูลการซื้อขายจากฐานข้อมูล MySQL ไปยัง Google Cloud Storage (GCS)
 2. นำข้อมูลการแสดงความคิดเห็นจากลูกค้าไปประมวลผลว่า ความคิดเห็นนั้นไปในทิศทางที่ดีหรือไม่ โดยใช้ PySpark ที่รันบนคลัสเตอร์ของ Google Dataproc
 3. นำข้อมูลการซื้อขายและข้อมูลการแสดงความคิดเห็นจากลูกค้าเข้าไปยัง Google Bigquery
 4. สร้าง Dashbord โดยใช้ Power BI เพื่อช่วยให้ทีมการตลาดและพัฒนาผลิตภัณฑ์เข้าใจความคิดเห็นของลูกค้าและพฤติกรรมการซื้อได้ดีขึ้น นำไปสู่การตัดสินใจทางธุรกิจที่มีประสิทธิภาพมากขึ้นและการปรับปรุงผลิตภัณฑ์ให้ตรงกับความต้องการของลูกค้า
 5. กระบวนการทั้งหมดถูกจัดการด้วย Apache Airflow และ Docker

## Source code
DAG file: [movie_project.py](https://github.com/saksit63/movie-project/blob/main/dags/movie_project.py)

PySpark file: [pyspark_script.py](https://github.com/saksit63/movie-project/blob/main/include/python/pyspark_script.py)


## Result
Airflow:

![Airlofw](https://github.com/saksit63/movie-project/blob/main/result/airflow_dag.png)

Bigquery:

![Bigquery](https://github.com/saksit63/movie-project/blob/main/result/bigquery.png)

Dashbord: 

![Dashboard](https://github.com/saksit63/movie-project/blob/main/result/movie_dashboard.png)


