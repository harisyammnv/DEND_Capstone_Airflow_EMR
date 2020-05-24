import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import argparse


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


parser = argparse.ArgumentParser()
parser.add_argument("--input", help="Location for Raw Data S3 Bucket")
parser.add_argument("--output", help="Location for Processed Data in S3")
args = parser.parse_args()
if args.input:
    input_bucket = args.input
else:
    input_bucket = 'dend-capstone-data'
if args.output:
    output_bucket = args.output
else:
    output_bucket = 'test-capstone-final'

udf_parse_port_of_entry = udf(lambda x: port_of_entry(x), StringType())
udf_parse_latitude = udf(lambda x: parse_latitude(x), FloatType())
udf_parse_longitude = udf(lambda x: parse_longitude(x), FloatType())
udf_parse_state_code = udf(lambda x: parse_state_code(x), StringType())
udf_parse_country_code = udf(lambda x: parse_country_code(x), StringType())
#
nations = spark.read.format('csv').load('s3://{}/raw/codes/nationality-codes.csv'.format(input_bucket), header=True, inferSchema=True)
nations_df = nations.selectExpr("Nationality as country","Code as country_abbr")
nations_df.write.mode("overwrite").parquet("s3://{}/lake/codes/country_code/".format(output_bucket))

#
ports = spark.read.format('csv').load('s3://{}/raw/codes/port-of-entry-codes.csv'.format(input_bucket), header=True, inferSchema=True)\
    .withColumn("port_of_entry", udf_parse_port_of_entry("Location"))

ports.write.mode("overwrite").parquet("s3://{}/lake/codes/port-of-entry-codes/".format(output_bucket))

us_airport = spark.read.format('csv').load('s3://{}/raw/codes/airport-codes.csv'.format(input_bucket), header=True, inferSchema=True)\
                        .withColumn("airport_latitude", udf_parse_latitude("coordinates"))\
                        .withColumn("airport_longitude", udf_parse_longitude("coordinates"))\
                        .withColumn("country", udf_parse_country_code("iso_region"))\
                        .withColumn("state_code", udf_parse_state_code("iso_region"))\
                        .withColumnRenamed("ident", "icao_code")\
                        .drop("coordinates", "gps_code", "local_code", "iso_region", "iso_country")

us_airport.write.mode("overwrite").parquet("s3://{}/lake/codes/airport_codes/".format(output_bucket))