import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_state(x):
    return x.strip().split('-')[-1]


udf_parse_state = udf(lambda x: parse_state(x), StringType())


demographics = spark.read.format('csv').load('s3://dend-capstone-data/raw/demographics/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')\
                .select("State", "City")\
                .withColumnRenamed("State", "state")\
                .withColumnRenamed("City", "city")


demographics.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/demographics/")