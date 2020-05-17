import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


visa = spark.read.format('csv').load('s3://dend-capstone-data/raw/visa-type.csv', header=True, inferSchema=True)
visa_df = visa.selectExpr("visa-type as visa_type","description as visa_type_description")
visa_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/visa-type/")

visa_port = spark.read.format('csv').load('s3://dend-capstone-data/raw/visa-type.csv', header=True, inferSchema=True)
visa_port_df = visa_port.selectExpr("Post as city","Code as city_code")
visa_port_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/visa-port/")