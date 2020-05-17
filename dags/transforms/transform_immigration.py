from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta
import sys


def to_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


def to_datetimefrstr(x):
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        return None


udf_to_datetimefrstr = udf(lambda x: to_datetimefrstr(x), DateType())
udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())

immigrant = spark.read.format('com.github.saurfang.sas.spark')\
                 .load('s3://de-capstone/raw/i94_immigration_data/i94_{}_sub.sas7bdat'.format(month_year))\
                 .selectExpr('cast(cicid as int) AS cicid', 'cast(i94res as int) AS from_country_code',
                             'cast(i94bir as int) AS age', 'cast(i94visa as int) AS visa_code',
                             'visapost AS visa_post', 'occup AS occupation',
                             'visatype AS visa_type', 'cast(biryear as int) AS birth_year', 'gender')\
                 .withColumn("i94_dt", F.lit(month_year))
#
immigrant.write.partitionBy("i94_dt").mode("append").parquet("s3://dend-capstone-data/lake/immigrantion/")