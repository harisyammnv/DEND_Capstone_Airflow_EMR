import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import argparse

"""
This python function is used for transforming the demographics data
"""


def parse_state(x):
    return x.strip().split('-')[-1]

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


udf_parse_state = udf(lambda x: parse_state(x), StringType())

# Transforming demographics data
# demographics >> us_cities_demographics DWH table

demographics = spark.read.format('csv').options(header='true', inferSchema='true',delimiter=';')\
    .load('s3://{}/raw/us-demographics/us-cities-demographics.csv'.format(input_bucket))\
    .withColumnRenamed("State", "state")\
    .withColumnRenamed("State Code", "state_code")\
    .withColumnRenamed("City", "city")\
    .withColumnRenamed("Median Age", "median_age")\
    .withColumnRenamed("Male Population", "male_population")\
    .withColumnRenamed("Female Population", "female_population")\
    .withColumnRenamed("Total Population", "total_population")\
    .withColumnRenamed("Number of Veterans", "num_of_veterans")\
    .withColumnRenamed("Foreign-born", "foreign_born")\
    .withColumnRenamed("Average Household Size", "avg_household_size") \
    .withColumnRenamed("State Code", "state_code") \
    .withColumnRenamed("Race", "predominant_race")\
    .withColumnRenamed("Count", "count")\


demographics.write.mode("overwrite").parquet("s3://{}/lake/demographics/".format(output_bucket))