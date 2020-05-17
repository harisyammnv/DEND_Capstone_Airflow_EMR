import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_latitude(x):
    y = x.strip().split(',')
    return float(y[0])


def parse_longitude(x):
    y = x.strip().split(',')
    return float(y[1])


def port_of_entry(x):
    return x.strip().split(', ')[0]


def parse_state_code(x):
    return x.strip().split('-')[-1]


def parse_country_code(x):
    return x.strip().split('-')[0]


udf_parse_port_of_entry = udf(lambda x: port_of_entry(x), StringType())
udf_parse_latitude = udf(lambda x: parse_latitude(x), FloatType())
udf_parse_longitude = udf(lambda x: parse_longitude(x), FloatType())
udf_parse_state_code = udf(lambda x: parse_state_code(x), StringType())
udf_parse_country_code = udf(lambda x: parse_country_code(x), StringType())
#
nations = spark.read.format('csv').load('s3://dend-capstone-data/raw/codes/nationality-codes.csv', header=True, inferSchema=True)
nations_df = nations.selectExpr("Nationality as country","Code as country_abbr")
nations_df.write.mode("overwrite").parquet("s3://dend-capstone-data/lake/codes/country_code/")

#
ports = spark.read.format('csv').load('s3://dend-capstone-data/raw/codes/port-of-entry-codes.csv', header=True, inferSchema=True)\
    .withColumn("port_of_entry", udf_parse_port_of_entry("Location"))

ports.write.mode("overwrite").parquet("s3://de-capstone/lake/codes/port-of-entry-codes/")

us_airport = spark.read.format('csv').load('s3://dend-capstone-data/raw/codes/airport-codes.csv', header=True, inferSchema=True)\
                        .withColumn("airport_latitude", udf_parse_latitude("coordinates"))\
                        .withColumn("airport_longitude", udf_parse_longitude("coordinates"))\
                        .withColumn("country", udf_parse_country_code("iso_region"))\
                        .withColumn("state_code", udf_parse_state_code("iso_region"))\
                        .withColumnRenamed("ident", "icao_code")\
                        .drop("coordinates", "gps_code", "local_code", "continent",
                              "iso_region", "iso_country")
us_airport.write.mode("overwrite").parquet("s3://de-capstone/lake/codes/airport_codes/")