import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_state(x):
    return x.strip().split('-')[-1]


udf_parse_state = udf(lambda x: parse_state(x), StringType())


demographics = spark.read.format('csv').options(header='true', inferSchema='true',delimiter=';')\
    .load('s3://dend-capstone-data/raw/us-demographics/us-cities-demographics.csv')\
    .withColumnRenamed("State", "state")\
    .withColumnRenamed("City", "city")\
    .withColumnRenamed("Median Age", "median_age")\
    .withColumnRenamed("Male Population", "male_population")\
    .withColumnRenamed("Female Population", "female_population")\
    .withColumnRenamed("Total Population", "total_population")\
    .withColumnRenamed("Number of Veterans", "num_of_veterans")\
    .withColumnRenamed("Foreign-born", "foreign_born")\
    .withColumnRenamed("Average Household Size", "avg_household_size")\
    .withColumnRenamed("Race", "predominant_race")\
    .withColumnRenamed("Count", "count")\


demographics.write.mode("overwrite").parquet("s3://test-capstone-final/lake/demographics/")