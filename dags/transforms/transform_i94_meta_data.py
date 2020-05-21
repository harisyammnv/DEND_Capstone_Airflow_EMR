import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_state_code(x):
    if x is not None and 'BPS' in x:
        return x.strip().split('(BPS)')[0].strip()
    elif x is not None and 'ARPT' in x:
        return x.strip().split('#ARPT')[0].strip()
    elif x is not None and 'SECTOR HQ' in x:
        return x.strip().split('(BP - SECTOR HQ)')[0].strip()
    elif x is not None and 'INTL' in x:
        return x.strip().split('#INTL')[0].strip()
    else:
        return x


udf_parse_state_code = udf(lambda x: parse_state_code(x), StringType())

i94_addr = spark.read.format('csv').load('s3://dend-capstone-data/raw/i94_meta_data/i94addr.csv', header=True, inferSchema=True)
i94_addr_df = i94_addr.selectExpr("i94_state_code as state_code","i94_state_name as state_name")
i94_addr_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/i94_meta_data/state_codes/")

i94_cit = spark.read.format('csv').load('s3://dend-capstone-data/raw/i94_meta_data/i94cit_i94res.csv', header=True, inferSchema=True)
i94_cit_df = i94_cit.selectExpr("i94_country_code as country_id","country_name as country")
i94_cit_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/i94_meta_data/country_codes/")

i94_mode = spark.read.format('csv').load('s3://dend-capstone-data/raw/i94_meta_data/i94_mode.csv', header=True, inferSchema=True)
i94_mode_df = i94_mode.selectExpr("i94_mode_code as mode_id","i94_mode as transportation_mode")
i94_mode_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/i94_meta_data/transportation/")

i94_port = spark.read.format('csv').load('s3://dend-capstone-data/raw/i94_meta_data/i94port_i94code.csv', header=True, inferSchema=True)
i94_port=i94_port.withColumn("port_state_cleaned", udf_parse_state_code("port_state"))
i94_port_df = i94_port.selectExpr("i94_port_code as port_code","port_city as city","port_state_cleaned as state_code")
i94_port_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/i94_meta_data/port_codes/")

i94_visa = spark.read.format('csv').load('s3://dend-capstone-data/raw/i94_meta_data/i94visa.csv', header=True, inferSchema=True)
i94_visa_df = i94_visa.selectExpr("i94_visa_code as visa_code","i94_visa as visa_purpose")
i94_visa_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/i94_meta_data/visa/")