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


def create_cast_select_exprs(sas_cols: list, schema_cols: list) -> list:
    exprs = []
    if sas_cols!='':
        exprs = ["{} AS {}".format(dfc,sc) for dfc, sc in zip(sas_cols, schema_cols)]
        return exprs


immigration = spark.read.format('com.github.saurfang.sas.spark')\
                 .load('s3://dend-capstone-data/raw/i94_immigration_data/i94_{}_sub.sas7bdat'.format(month_year))
immigration_df = immigration.drop('validres','delete_days','delete_mexl','delete_dup','delete_recdup','delete_visa',
                                  'dtadfile', 'occup', 'entdepa', 'entdepd', 'entdepu','dtaddto')


sas_columns = ['cast(cicid as int)','cast(i94yr as int)','cast(i94mon as int)','cast(i94cit as int)',
               'cast(i94res as int)','i94port','cast(arrdate as double)','cast(i94mode as int)',
               'i94addr','cast(depdate as double)','cast(i94bir as int)','cast(i94visa as int)',
               'cast(count as int)','visapost','matflag','cast(biryear as int)',
               'gender','insnum','airline','cast(admnum as float)','fltno','visatype']

schema_columns = ['cicid','entry_year','entry_month','country_id','res_id','port_id','arrival_date',
                  'mode_id','state_code','departure_date','age','visa_reason_id','count','visa_post',
                  'matched_flag','birth_year','gender','ins_num','airline_abbr','admission_num','flight_no','visa_type']


immigration_df = immigration_df.selectExpr(create_cast_select_exprs(sas_columns,schema_columns))


immigration_df.write.partitionBy("entry_year","entry_month").mode("append").parquet("s3://dend-capstone-data/lake/immigrantion/")