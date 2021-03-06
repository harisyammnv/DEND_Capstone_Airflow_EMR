from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta
import sys
import argparse

"""
This file is used for transforming the SAS immigration Data with the help of EMR add steps operator
"""


def create_spark_session(app_name='immigration_transform'):
    """Creates the spark session"""
    spark = SparkSession\
    .builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
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


def to_datetime(x):
    """ function to provide the datetime using sas delta"""
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


def to_datetime_frm_str(x):
    """extract datetime from string"""
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        return None


udf_to_datetime_frm_str = udf(lambda x: to_datetime_frm_str(x), DateType())
udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())


def create_cast_select_exprs(sas_cols, schema_cols):
    """select expression filter for creating the immigration table columns"""
    if sas_cols != '':
        exprs = ["{} AS {}".format(dfc,sc) for dfc, sc in zip(sas_cols, schema_cols)]
    else:
        raise ValueError('Cannot create Select Expression without proper header')
    return exprs


spark = create_spark_session(app_name='transform_i94_sas_data')
logger = create_logger(spark)

parser = argparse.ArgumentParser(description='Argument Usage')
parser.add_argument("--input", help="Location for Raw Data S3 Bucket")
parser.add_argument("--output", help="Location for Processed Data in S3")
parser.add_argument("--month", help="Month indicator for Data in S3")
parser.add_argument("--year", help="Year indicator for Data in S3")
args = parser.parse_args()
if args.input:
    input_bucket = args.input
else:
    input_bucket = 'dend-capstone-data'
if args.output:
    output_bucket = args.output
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

immigration = spark.read.format('com.github.saurfang.sas.spark').option("dateFormat", "yyyyMMdd")\
                     .load('s3://{}/raw/i94_immigration_data/i94_{}_sub.sas7bdat'.format(input_bucket, data_month+data_year))\
                     .withColumn("arrival_date", udf_to_datetime_sas("arrdate"))\
                     .withColumn("departure_date", udf_to_datetime_sas("depdate"))\
                     .withColumn("departure_deadline", F.lit(udf_to_datetime_frm_str("dtaddto"))) \
                     .withColumn("month_year", F.lit(data_month+'_'+data_year))

immigration_df = immigration.drop('validres','delete_days','delete_mexl','delete_dup','delete_recdup','depdate','delete_visa',
                                  'arrdate','dtadfile','dtaddto' ,'occup', 'entdepa', 'entdepd', 'entdepu')

sas_columns = ['cast(cicid as int)','cast(i94yr as int)','cast(i94mon as int)','cast(i94cit as int)',
               'cast(i94res as int)','i94port','arrival_date','cast(i94mode as int)',
               'i94addr','departure_date','departure_deadline','cast(i94bir as int)','cast(i94visa as int)',
               'cast(count as int)','visapost','matflag','cast(biryear as int)',
               'gender','insnum','airline','cast(admnum as float)','fltno','visatype',"month_year"]

schema_columns = ['cicid','entry_year','entry_month','country_id','res_id','port_id','arrival_date',
                  'mode_id','state_code','departure_date','departure_deadline','age','visa_reason_id','count','visa_post',
                  'matched_flag','birth_year','gender','ins_num','airline_abbr','admission_num','flight_no','visa_type','month_year']

immigration_df = immigration_df.selectExpr(create_cast_select_exprs(sas_columns,schema_columns))
# Tranformed SAS data to Parquet file partitioned by month and year combined
# SAS Data >> immigration fact DWH table
immigration_df.write.partitionBy("month_year").mode("append").\
    parquet("s3://{}/lake/immigration/".format(output_bucket))