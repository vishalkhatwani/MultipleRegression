

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import psycopg2

def intitalize():
    spark = SparkSession.builder\
                .master("local[2]")\
                .appName('housing_data_ETL')\
                .config("spark.executor.logs.rolling.time.interval", "daily")\
                .getOrCreate()
    return spark

# def pgadmincon():
#    print("initializing pgAdmin connection .....")
#    conn = psycopg2.connect(host="localhost", database="WorkingDatabase", user="postgres", password = "postgres")
#    cur1 = conn.cursor()
#    return cur1


def defineschema():
    schema = StructType([
                StructField('ID', LongType(), True),
                StructField("Date", StringType(), True),
                StructField("Price", FloatType(), True),
                StructField("Bedrooms", FloatType(), True),
                StructField("Bathrooms", FloatType(), True),
                StructField("Sqft_Apt", FloatType(), True),
                StructField("Sqft_Lot", FloatType(), True),
                StructField("Floors", FloatType(), True),
                StructField("Waterfront", IntegerType(), True),
                StructField("view", IntegerType(), True),
                StructField("condition", IntegerType(), True),
                StructField("grade", IntegerType(), True),
                StructField("Sqft_above", IntegerType(), True),
                StructField("Sqft_basement", IntegerType(), True),
                StructField("YearBuilt", IntegerType(), True),
                StructField("YearRenovated", IntegerType(), True),
                StructField("ZipCode", IntegerType(), True),
                StructField("Lat", FloatType(), True),
                StructField("Long", FloatType(), True),
                StructField("Sqft_Apt2", FloatType(), True),
                StructField("Sqft_Lot2", FloatType(), True)
                ])
    return schema

def dataload(spark, pathname, schema):
    df1 = spark.read.format("csv").option("header", "true").schema(schema).load(pathname)
    return df1

def datacleaning(dfp):
    dfp1 = dfp.repartition(5)
    dfp1 = dfp1.drop("Sqft_Apt2", "Sqft_Lot2")
    dfp2 = dfp1.withColumn('DateClean', ltrim(rtrim(substring(dfp1.Date, 1, 8))))
    dfp2 = dfp2.drop("Date")
    return dfp2

def createtable(cursorname):
    try:
        cursorname.execute("Create table IF NOT EXISTS houseprice (\
                                    ID BIGINT,\
                                    Price Float,\
                                    Bedrooms Float,\
                                    Bathrooms Float,\
                                    Sqft_Apt Float,\
                                    Sqft_Lot Float,\
                                     Floors Float,\
                                    Waterfront INT,\
                                    view INT,\
                                    condition INT,\
                                    grade INT,\
                                    Sqft_above INT,\
                                    Sqft_basement INT,\
                                    YearBuilt INT,\
                                    YearRenovated INT,\
                                    ZipCode INT,\
                                    Lat Float,\
                                    Long Float,\
                                    DateClean Varchar(10)\
                                    );")
        print("New Table Created in PostGres")
    except:
        print("There was an error creating a new table in postgres..")


def write_to_pgadmin(dfn):
    data_seq = [tuple(x) for x in dfn.select('*').collect()]
    record_list_template = ','.join(['%s'] * len(data_seq))
    insert_query = "INSERT INTO houseprice (ID, Price, Bedrooms, Bathrooms, Sqft_Apt, Sqft_Lot, Floors,Waterfront,\
                   view, condition, grade, Sqft_above, Sqft_basement, YearBuilt, YearRenovated,ZipCode, Lat, Long,\
                   DateClean) VALUES {}".format(record_list_template)
    print("inserting data into PostGres")
    return insert_query, data_seq

def main(insertquery=None):
    print("initiatiing spark...")
    spark1 = intitalize()
    print("spark initiated")
    schema1 = defineschema()
    print("schema defined...")
    print("Starting the data load")
    df = dataload(spark1, "/Users/vishalkhatwani/Documents/python/Multiple Regression/kc_house_data.csv", schema1)
    print("Data load finished...")
    df.show(3)
    print("starting Data Cleaning")
    df1 = datacleaning(df)
    print("Data Cleaning Finished")
    df1.show(5)
    print("initializing pgAdmin connection .....")
    conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    createtable(cur)
    insert_query, house_price_seq = write_to_pgadmin(df1)
    cur.execute(insert_query, house_price_seq)
    print("data inserted into postgres..")
    conn.commit()
    cur.close()
    print("ETL job finished..")

main()



''''''
