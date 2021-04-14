from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *

Spark = SparkSession.builder\
                    .master("local[5]")\
                    .appName("test load")\
                    .config("spark.jars", "/Users/vishalkhatwani/opt/anaconda3/bin/postgresql-42.2.19.jar")\
                    .getOrCreate()

df1 = Spark.read.format("jdbc").\
        options(url="jdbc:postgresql://localhost:5432/postgres",
                 query="(Select * from public.houseprice where dateclean >= '20140901')",
                 user="postgres",
                 password="postgres",
                 driver="org.postgresql.Driver").\
        load()

df1.printSchema()

print(df1.select('*').count())


