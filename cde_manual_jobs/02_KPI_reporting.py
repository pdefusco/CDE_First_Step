from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql.functions import to_date

import configparser
import sys

config = configparser.ConfigParser()
config.read('/app/mount/cde_examples.ini')
s3BucketPath = config['CDE-examples']['s3BucketPath'].replace('"','').replace("\'",'')
prefix = config['CDE-examples']['userPrefix'].replace('"','').replace("\'",'')

## Launching Spark Session

spark = SparkSession\
    .builder\
    .appName("DataExploration")\
    .config("spark.yarn.access.hadoopFileSystems", s3BucketPath)\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region",config['CDE-examples']['region'])\
    .getOrCreate()

## Creating Spark Dataframe from raw CSV datagov

df = spark.read.option('inferschema','true').csv(
  s3BucketPath + "/cde_first_steps/LoanStats_2015_subset_112822.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

## WHat is the target variable and what does it define?
df.select("loan_status").distinct().show()

## Types of loan status
print(df.groupBy('loan_status').count().show())

df = df.withColumn("is_default", when((df["loan_status"] == "Charged Off")|(df["loan_status"] == "Default"), 1).otherwise(0))
#Checking that we have correctly replaced values
df.select("is_default").show()
df.select("is_default").dtypes
#Check the exact total of all loans labeled to default matches with the sum of the original two values used above (Charged Off and Default)
df.select(F.sum("is_default")).collect()[0][0]

## What is the monthly total loan volume in dollars and what is the montjly average loan size?

#The original issue date attribute
df.select("issue_d").show(4)

#We need to cast the issue date from string to month (all loan applications in the dataset occurred in 2015 so we don't need the year):
df.selectExpr("from_unixtime(unix_timestamp(issue_d,'MMM-yyyy'),'MM') as issue_month").show(4)

df = df.withColumn("issue_month",F.from_unixtime(F.unix_timestamp(F.col("issue_d"),'MMM-yyyy'),'MM'))

df.select("issue_month").distinct().show()

## How many loans defaulted for each month (all data is 2015):
df.groupby('issue_month').sum('is_default').na.drop().sort(F.asc('issue_month')).show()

from pyspark.sql.functions import sum as _sum

#by using like function
df.groupBy("issue_month","loan_status").\
count().\
filter(F.lower(F.col("loan_status")).like("late%")).\
groupby('issue_month').\
sum().\
sort(F.asc('issue_month')).\
show()

df_late = df.groupBy("issue_month","loan_status").\
count().\
filter(F.lower(F.col("loan_status")).like("late%")).\
groupby('issue_month').\
sum().\
sort(F.asc('issue_month'))

#by using like function
df_delinq = df.groupBy("issue_month").\
max("inq_last_6mths").\
na.drop().\
sort(F.asc('issue_month'))

#This time we need to cast the attribute we are working with to numeric before we can create a similar dataframe:
df = df.withColumn('loan_amnt', F.col('loan_amnt').cast('int'))

#by using like function
df_ann_inc = df.groupBy("issue_month").\
mean("loan_amnt").\
na.drop().\
sort(F.asc('issue_month'))

df_delinq.alias('a').join(df_ann_inc.alias('b'),F.col('b.issue_month') == F.col('a.issue_month')).\
join(df_late.alias('c'), F.col('b.issue_month') == F.col('c.issue_month')).\
select(F.col('a.issue_month'), F.col('a.max(inq_last_6mths)'), F.col('b.avg(loan_amnt)'), F.col('c.sum(count)').alias('default_count')).\
show()

## Spark sql

spark.sql("show databases").show()

spark.sql("show tables").show()

df.write.format('parquet').mode("overwrite").saveAsTable('default.' + prefix + '_LC_table')

#Running SQL like queries on the dataframe
group_by_grade = spark.sql("SELECT grade, MEAN(loan_amnt) FROM " + prefix + "_LC_table WHERE grade IS NOT NULL GROUP BY grade ORDER BY grade")

group_by_grade.show()

group_by_subgrade = spark.sql("SELECT sub_grade, MEAN(loan_amnt), MEAN(annual_inc), SUM(is_default) FROM " + prefix + "_LC_table GROUP BY sub_grade ORDER BY sub_grade")

