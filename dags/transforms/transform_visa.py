import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf

import argparse

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

if input_bucket != '' and output_bucket != '':
    visa = spark.read.format('csv').load('s3://{}/raw/us-visa/visa-type.csv'.format(input_bucket), header=True, inferSchema=True)
    visa_df = visa.withColumnRenamed("visa-type", "visa_type").withColumnRenamed("description", "visa_type_description")
    visa_df.write.mode("overwrite").parquet("s3://{}/lake/visa-type/".format(output_bucket))

    visa_port = spark.read.format('csv').load('s3://{}/raw/us-visa/visa-issuing-ports.csv'.format(input_bucket), header=True, inferSchema=True)
    visa_port_df = visa_port.selectExpr("Post as port_of_issue","Code as visa_post")
    visa_port_df.write.mode("overwrite").parquet("s3://{}/lake/visa-issue-port/".format(output_bucket))