import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


visa = spark.read.format('csv').load('s3://dend-capstone-data/raw/us-visa/visa-type.csv', header=True, inferSchema=True)
visa_df = visa.withColumnRenamed("visa-type", "visa_type").withColumnRenamed("description", "visa_type_description")
visa_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/visa-type/")

visa_port = spark.read.format('csv').load('s3://dend-capstone-data/raw/us-visa/visa-issuing-ports.csv', header=True, inferSchema=True)
visa_port_df = visa_port.selectExpr("Post as port_of_issue","Code as visa_post")
visa_port_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/visa-issue-port/")