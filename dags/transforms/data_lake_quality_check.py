from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta
import sys
import argparse


def check_data_quality_livy(path, table, logger):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        logger.warn("Data Quality check for - {} SUCCESS".format(table))
    else:
        logger.warn("Data Quality check for - {} FAIL".format(table))
        raise ValueError("Data Quality Check not passed")


def create_spark_session(app_name='immigration_data_check'):
    """Creates the spark session"""
    spark = SparkSession\
    .builder\
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
    .appName(app_name)\
    .getOrCreate()
    return spark


def create_logger(spark):
    """Creates the logger """
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")
    return logger


parser = argparse.ArgumentParser(description='Argument Usage')
parser.add_argument("--data", help="Location for S3 Data Bucket")
parser.add_argument("--livy_session", help="Calling the script using Livy REST [Yes/No]")
parser.add_argument("--aws_key", help="AWS access key needed for S3")
parser.add_argument("--aws_secret", help="AWS secret key needed for S3")
parser.add_argument("--month", help="Month indicator for Data in S3")
parser.add_argument("--year", help="Year indicator for Data in S3")
args = parser.parse_args()
if args.livy_session == "No":
    spark = create_spark_session(app_name='data_quality_check')
    logger = create_logger(spark)
    if args.data:
        output_bucket = args.data
    else:
        output_bucket = 'test-capstone-final'
    if args.month:
        data_month = args.month
    else:
        raise ValueError("Cannot open S3 SAS File without a valid file indicator like month")
    if args.year:
        data_year = args.year
    else:
        raise ValueError("Cannot open S3 SAS File without a valid file indicator like year")

    data_location = "s3://{}/lake/immigration/".format(args.data)
    df = spark.read.parquet(data_location).filter("month_year = '{}'".format(data_month+'_'+data_year))
    if len(df.columns) > 0 and df.count() > 0:
        logger.warn("Data Quality check for - {} SUCCESS".format("immigration_data"))
    else:
        logger.warn("Data Quality check for - {} FAIL".format("immigration_data"))
else:
    logger = create_logger(spark)
    access_key = args.aws_key
    secret_key = args.aws_secret
    out_bucket = args.data
    logger.info("access - "+access_key)
    logger.info("key - "+secret_key)
    logger.info("s3://{}/lake/codes/country_code/".format(out_bucket))

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

    check_data_quality_livy("s3://{}/lake/codes/country_code/".format(out_bucket), 'country_code', logger)
    check_data_quality_livy("s3://{}/lake/codes/port-of-entry-codes/".format(out_bucket), 'port-of-entry-codes', logger)
    check_data_quality_livy("s3://{}/lake/codes/airport_codes/".format(out_bucket), 'airport_codes', logger)
    check_data_quality_livy("s3://{}/lake/codes/airline_codes/".format(out_bucket), 'airline_codes', logger)
    check_data_quality_livy("s3://{}/lake/demographics/".format(args.data), 'demographics', logger)
    check_data_quality_livy("s3://{}/lake/i94_meta_data/transportation/".format(out_bucket), 'transportation', logger)
    check_data_quality_livy("s3://{}/lake/i94_meta_data/country_codes/".format(out_bucket), 'country_codes', logger)
    check_data_quality_livy("s3://{}/lake/i94_meta_data/state_codes/".format(out_bucket), 'state_codes', logger)
    check_data_quality_livy("s3://{}/lake/i94_meta_data/port_codes/".format(out_bucket), 'port_codes', logger)
    check_data_quality_livy("s3://{}/lake/i94_meta_data/visa/".format(out_bucket), 'visa', logger)
    check_data_quality_livy("s3://{}/lake/visa-issue-port/".format(out_bucket), 'visa-issue-port', logger)
    check_data_quality_livy("s3://{}/lake/visa-type/".format(out_bucket), 'visa-type', logger)





