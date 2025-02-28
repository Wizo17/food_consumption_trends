# Veuillez n'inclure ici que du code en Python. Quand vous cliquez sur EXÉCUTER ou ENREGISTRER, une procédure stockée est créée à l'aide de ce code.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, when, regexp_replace, array_contains, size, lit, udf
from pyspark.sql.types import FloatType, ArrayType, StringType
import json

# Global conf
PROJECT_ID = "analytics-trafic-idfm"
DATASET_NAME = "food_consumption_trends"
RAW_TABLE = "raw_data_off"

spark = None

def main():
  init_spark_session()
  if spark is not None:
    print("INFO - Start transformation")
    print(f"INFO - Load raw data in {PROJECT_ID}.{DATASET_NAME}.{RAW_TABLE}")

    raw_df = load_bigquery_data(f"{PROJECT_ID}.{DATASET_NAME}.{RAW_TABLE}")
    if raw_df is not None:
      print("INFO - Number of lines:", raw_df.count())
      raw_df.printSchema()
      print(f"INFO - Transform data")
      transform_df = transform_data(raw_df)


    close_spark_session()


############ Init spark session ############
def init_spark_session():
  try:
    global spark
    spark = SparkSession.builder \
        .appName("OpenFoodFactsProcessing") \
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
        .getOrCreate()
    
    print(f"INFO - Spark initialized with {spark.version}")
  except Exception as e:
    print(f"ERROR - Init spark session: {str(e)}")




############ Close spark session ############
def close_spark_session():
  global spark
  if spark is not None:
    spark.stop()
    print(f"INFO - Spark session closed")



############ Load raw data from bigquery ############
def load_bigquery_data(table_data_path):
  try:
    raw_df = spark.read \
    .format("bigquery") \
    .option("table", table_data_path) \
    .load()
    
    return raw_df
  except Exception as e:
    print(f"ERROR - Load data: {str(e)}")
    return None


############ Load raw data from bigquery ############
def load_bigquery_data(table_data_path):
  try:
    raw_df = spark.read \
    .format("bigquery") \
    .option("table", table_data_path) \
    .load()
    
    return raw_df
  except Exception as e:
    print(f"ERROR - Load data: {str(e)}")
    return None


def transform_data(dataframe):
    try:
        print(f"INFO - Cleaning data")
        clean_df = dataframe \
            .filter(col("product_name").isNotNull()) \
            .filter(col("nutriscore_grade").isNotNull() | col("nutrition_grade_fr").isNotNull()) \
            .withColumn("nutriscore", when(col("nutriscore_grade").isNotNull(), col("nutriscore_grade")).otherwise(col("nutrition_grade_fr"))) \
            .withColumn("product_name_clean", regexp_replace(col("product_name"), "[^a-zA-Z0-9àáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆŠŽ ,.-]", ""))

    except Exception as e:
        print(f"ERROR - Transform data: {str(e)}")
        return None


main()