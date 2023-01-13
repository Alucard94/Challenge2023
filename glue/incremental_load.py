import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, FloatType, DateType, StructType, StructField, IntegerType
from pyspark.sql.functions import col, year, month, date_format, current_date, concat,  when
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
import random
import string

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

CURRENT_DAY = str(datetime.date.today()).replace('-', '')[:8]
BUCKET_NAME = "s3://landing20230113"
BUCKET_VEHICLE_ORIGIN_PATH = f"{BUCKET_NAME}/vehicle/{CURRENT_DAY}/*.parquet"
BUCKET_OP_PERIOD_ORIGIN_PATH = f"{BUCKET_NAME}/operating_period/{CURRENT_DAY}/*.parquet"
BUCKET_FINAL =  "s3://staging20230113"

df_hist_vehicle = sqlContext.read.parquet(BUCKET_VEHICLE_ORIGIN_PATH)
df_hist_op_period = sqlContext.read.parquet(BUCKET_OP_PERIOD_ORIGIN_PATH)

# Vehicle table
df_vehicle = df_hist_vehicle
df_vehicle = df_vehicle.withColumn('loaded_date', current_date())
df_vehicle = df_vehicle.withColumn('year', year(col('loaded_date')))
df_vehicle = df_vehicle.withColumn('month', month(col('loaded_date')))
df_vehicle = df_vehicle.withColumn('day', date_format(col('loaded_date'), 'd'))

df_vehicle.write.partitionBy("loaded_date").mode("append").parquet(BUCKET_FINAL+"/vehicle")

# Operating period table
df_op = df_hist_op_period
df_op = df_op.withColumn('loaded_date', current_date())
df_op = df_op.withColumn('year', year(col('loaded_date')))
df_op = df_op.withColumn('month', month(col('loaded_date')))
df_op = df_op.withColumn('day', date_format(col('loaded_date'), 'd'))

df_op.write.partitionBy("loaded_date").mode("append").parquet(BUCKET_FINAL+"/operating_period")
