from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.sql import functions as F
from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession

import configparser

config = configparser.ConfigParser()
config.read('/app/mount/cde_examples.ini')
s3BucketPath = config['CDE-examples']['s3BucketPath'].replace('"','').replace("\'",'')
prefix = config['CDE-examples']['userPrefix'].replace('"','').replace("\'",'')

## Creating new Spark session

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.yarn.access.hadoopFileSystems", s3BucketPath)\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", config['CDE-examples']['region'])\
    .getOrCreate()

## Loading data from Spark table we created previously

df = spark.sql("SELECT * FROM default." + prefix + "_LC_Table")

#Creating list of categorical and numeric features
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in') or item[1].startswith('dou')]

df = df.select(*num_cols)

df = df.dropna()

df = df.select(['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal', 'is_default'])

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

#Creates a Pipeline Object including One Hot Encoding of Categorical Features
def make_pipeline(spark_df):

    for c in spark_df.columns:
        spark_df = spark_df.withColumn(c, spark_df[c].cast("float"))

    stages= []

    cols = ['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal']

    #Assembling mixed data type transformations:
    assembler = VectorAssembler(inputCols=cols, outputCol="features", handleInvalid='skip')
    stages += [assembler]

    #Scaling features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    stages += [scaler]

    #Logistic Regression
    lr = LogisticRegression(featuresCol='scaledFeatures', labelCol='is_default', maxIter=10, regParam=0.3, elasticNetParam=0.4)
    stages += [lr]

    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(spark_df)
    out_df = pipelineModel.transform(spark_df)

    return out_df, pipelineModel


df_model, pipelineModel = make_pipeline(df)

input_data = df_model.rdd.map(lambda x: (x["is_default"], float(x['probability'][1])))

## Outputting predictions from Logistic Regression
predictions = spark.createDataFrame(input_data, ["is_default", "probability"])

predictions.write.format('parquet').mode("overwrite").saveAsTable('default.' + prefix + '_LC_model_scoring')

## Storing Model Pipeline to S3
pipelineModel.write().overwrite().save(s3BucketPath + "/" + prefix + "_pipeline")

