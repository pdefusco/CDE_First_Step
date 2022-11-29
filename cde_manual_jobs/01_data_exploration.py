from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/cde_examples.ini')
s3BucketPath = config['CDE-examples']['s3BucketPath'].replace('"','').replace("\'",'')

## Launching Spark Session
spark = SparkSession\
    .builder\
    .appName("DataExploration")\
    .config("spark.yarn.access.hadoopFileSystems", s3BucketPath)\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", config['CDE-examples']['region'])\
    .getOrCreate()

## Creating Spark Dataframe from raw CSV datagov
df = spark.read.option('inferschema','true').csv(
  s3BucketPath + "/cde_first_steps/LoanStats_2015_subset_112822.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

## Showing Different Loan Status Values
df.select("loan_status").distinct().show()

## Types of Loan Status Aggregated by Count

print(df.groupBy('loan_status').count().show())

