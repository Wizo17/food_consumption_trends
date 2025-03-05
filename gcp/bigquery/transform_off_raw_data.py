# Veuillez n'inclure ici que du code en Python. Quand vous cliquez sur EXÉCUTER ou ENREGISTRER, une procédure stockée est créée à l'aide de ce code.
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, when, regexp_replace, array_contains, size, lit, udf
from pyspark.sql.types import FloatType, ArrayType, StringType
import json
import ast

# Global conf
PROJECT_ID = "analytics-trafic-idfm"
DATASET_NAME = "food_consumption_trends"
RAW_TABLE = "raw_data_off"
PROCESS_TABLE = "processed_data_off"

spark = None

def main():
  init_spark_session()
  if spark is not None:
    print("INFO - Start transformation")
    print(f"INFO - Load raw data in {PROJECT_ID}.{DATASET_NAME}.{RAW_TABLE}")

    raw_df = load_bigquery_data(f"{PROJECT_ID}.{DATASET_NAME}.{RAW_TABLE}")
    if raw_df is not None:
      print("INFO - Number of lines:", raw_df.count())
      # raw_df.printSchema()

      print(f"INFO - Transform data")
      transform_df = transform_data(raw_df)

      print(f"INFO - Save data in bigquery")
      transform_df.printSchema()
      write_to_bq(transform_df, PROJECT_ID, DATASET_NAME, PROCESS_TABLE)

    close_spark_session()


############ Init spark session ############
def init_spark_session():
  try:
    global spark
    spark = SparkSession.builder \
        .appName("OpenFoodFactsProcessing") \
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
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


############ Compute a health score ############
def calculate_health_score(nutriscore, additives_list):
  # Base score (a=5, b=4, c=3, d=2, e=1)
  base_score = 0
  if nutriscore:
    if nutriscore.lower() == 'a':
      base_score = 5
    elif nutriscore.lower() == 'b':
      base_score = 4
    elif nutriscore.lower() == 'c':
      base_score = 3  
    elif nutriscore.lower() == 'd':
      base_score = 2
    elif nutriscore.lower() == 'e':
      base_score = 1
    
  # Penalty if the product contains one or more addictives
  additives_penalty = 0
  if additives_list:
    if isinstance(additives_list, str):
      additives_list = json.loads(additives_list.replace("'", "\"")) if "[" in additives_list else []
    if isinstance(additives_list, list):
          additives_penalty = min(len(additives_list) * 0.1, 2)  # Penalty max : 2 points
    
  # Final score
  final_score = max(base_score - additives_penalty, 0)
  return round(final_score, 2)

# Save udf function
health_score_udf = udf(calculate_health_score, FloatType())


############ Extract nutrients from json ############
def extract_nutrient(nutriments_json, nutrient_name):
    if nutriments_json:
        try:
            nutrients = ast.literal_eval(nutriments_json)
            return nutrients.get(nutrient_name)
        except:
            return None
    return None

# Save udf function
extract_nutrient_udf = udf(extract_nutrient, FloatType())


############ Transform off data ############
def transform_data(dataframe):
    try:
        print(f"INFO - Cleaning data")
        clean_df = dataframe \
            .filter(col("product_name").isNotNull()) \
            .withColumn("nutriscore_grade", when(col("nutriscore_grade").isin("a", "b", "c", "d", "e"), col("nutriscore_grade")).otherwise(None)) \
            .withColumn("nutrition_grade_fr", when(col("nutrition_grade_fr").isin("a", "b", "c", "d", "e"), col("nutrition_grade_fr")).otherwise(None)) \
        
        adapted_df = clean_df \
            .withColumn("nutriscore", when(col("nutriscore_grade").isNotNull(), col("nutriscore_grade")).otherwise(col("nutrition_grade_fr"))) \
            .withColumn("product_name_clean", regexp_replace(col("product_name"), "[^a-zA-Z0-9àáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆŠŽ ,.-]", "")) \
            .withColumn("additives_list", col("additives_tags"))

        # Compute health_score
        enriched_df = adapted_df \
          .withColumn("health_score", health_score_udf(col("nutriscore"), col("additives_list")))

        # Flag ultra processed
        enriched_df = enriched_df \
          .withColumn("is_ultra_processed", 
                    when((size(col("additives_list")) > 3) | 
                          (col("nutriscore").isin(["d", "e", "D", "E"])), 
                          lit(True)).otherwise(lit(False)))

        # Extract nutrients
        nutrient_list = ["energy_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g"]
        for nutrient in nutrient_list:
            enriched_df = enriched_df.withColumn(
                f"{nutrient.replace('-', '_')}", 
                extract_nutrient_udf(col("nutriments"), lit(nutrient))
            )

        enriched_df = enriched_df.withColumn(
              "is_bio", 
              when(
                  (col("_keywords").isNotNull() & array_contains(col("_keywords"), "bio")) |
                  (col("_keywords").isNotNull() & array_contains(col("_keywords"), "organic")) |
                  (col("product_name_clean").contains("bio")) | 
                  (col("product_name_clean").contains("organic")),
                  lit(True)
              ).otherwise(lit(False))
          )

        final_df = enriched_df \
          .withColumn("health_score", when(col("health_score").isNotNull(), col("health_score")).otherwise(0.0)) \
          .withColumn("is_ultra_processed", when(col("is_ultra_processed").isNotNull(), col("is_ultra_processed")).otherwise(False)) \
          .withColumn("energy_100g", when(col("energy_100g").isNotNull(), col("energy_100g")).otherwise(0.0)) \
          .withColumn("fat_100g", when(col("fat_100g").isNotNull(), col("fat_100g")).otherwise(0.0)) \
          .withColumn("saturated_fat_100g", when(col("saturated_fat_100g").isNotNull(), col("saturated_fat_100g")).otherwise(0.0)) \
          .withColumn("carbohydrates_100g", when(col("carbohydrates_100g").isNotNull(), col("carbohydrates_100g")).otherwise(0.0)) \
          .withColumn("sugars_100g", when(col("sugars_100g").isNotNull(), col("sugars_100g")).otherwise(0.0)) \
          .withColumn("fiber_100g", when(col("fiber_100g").isNotNull(), col("fiber_100g")).otherwise(0.0)) \
          .withColumn("proteins_100g", when(col("proteins_100g").isNotNull(), col("proteins_100g")).otherwise(0.0)) \
          .withColumn("salt_100g", when(col("salt_100g").isNotNull(), col("salt_100g")).otherwise(0.0)) \
          .withColumn("is_bio", when(col("is_bio").isNotNull(), col("is_bio")).otherwise(False))
        
        # final_df.show(10)
        return final_df
    except Exception as e:
        print(f"ERROR - Transform data: {str(e)}")
        return None


############ Save data in bigquery table ############
def write_to_bq(dataframe, gcp_project_id, bigquery_dataset, table_name, write_mode="overwrite"):
    """
    Writes a PySpark DataFrame to Google BigQuery.
    
    Parameters:
        dataframe (pyspark.sql.DataFrame): The PySpark DataFrame to write.
        gcp_project_id (str): The Google Cloud project ID.
        bigquery_dataset (str): The BigQuery dataset name.
        table_name (str): The target BigQuery table name.
        write_mode (str): Writing mode. Default is "overwrite".
    
    Returns:
        bool: True if writing is successful, False otherwise.
    """

    if not isinstance(dataframe, DataFrame):
      print(f"ERROR - Type of df {type(dataframe)}")
      print(f"ERROR - dataframe must be a PySpark DataFrame. Convert your Pandas DataFrame using spark.createDataFrame(df)")
      return False
    
    # Construct full table path
    table_path = f"{gcp_project_id}.{bigquery_dataset}.{table_name}"
    
    try:   
        # Configure write options
        write_options = {
            "table": table_path,
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_TRUNCATE_DATA",
            "writeMethod": "direct"
        }
        
        # Write to BigQuery
        dataframe.write.format("bigquery") \
            .options(**write_options) \
            .mode(write_mode) \
            .save()
            
        print(f"INFO - Table `{table_path}` written successfully")
        return True
        
    except Exception as e:
        print(f"ERROR - Writing to BigQuery: {str(e)}")
        return False

main()