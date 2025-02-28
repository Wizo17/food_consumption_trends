from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, when, regexp_replace, array_contains, size, lit, udf
from pyspark.sql.types import FloatType, ArrayType, StringType
import json

# Initialisation de Spark avec les configurations pour BigQuery
spark = SparkSession.builder \
    .appName("OpenFoodFactsProcessing") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Configuration pour accéder à BigQuery
project_id = "votre-project-id"
dataset_id = "open_food_facts"
table_id = "raw_data"

# Extraction des données depuis BigQuery
raw_df = spark.read \
    .format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.{table_id}") \
    .load()

# Vérification du schéma et des données
print("Nombre total de produits:", raw_df.count())
raw_df.printSchema()

# Nettoyage des données
clean_df = raw_df \
    .filter(col("product_name").isNotNull()) \
    .filter(col("nutriscore_grade").isNotNull() | col("nutrition_grade_fr").isNotNull()) \
    .withColumn("nutriscore", when(col("nutriscore_grade").isNotNull(), col("nutriscore_grade")).otherwise(col("nutrition_grade_fr"))) \
    .withColumn("product_name_clean", regexp_replace(col("product_name"), "[^a-zA-Z0-9àáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆŠŽ ,.-]", ""))

# Extraire les additifs et les transformer en liste
clean_df = clean_df \
    .withColumn("additives_list", col("additives_tags"))

# Fonction UDF pour calculer un score de santé basé sur le Nutri-Score et la présence d'additifs
def calculate_health_score(nutriscore, additives_list):
    # Base score selon Nutri-Score (a=5, b=4, c=3, d=2, e=1)
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
    
    # Pénalité pour les additifs
    additives_penalty = 0
    if additives_list:
        additives_penalty = min(len(additives_list) * 0.1, 2)  # Max pénalité de 2 points
    
    # Score final
    final_score = max(base_score - additives_penalty, 0)
    return round(final_score, 2)

# Enregistrement de l'UDF
health_score_udf = udf(calculate_health_score, FloatType())

# Application du calcul du score de santé
enriched_df = clean_df \
    .withColumn("health_score", health_score_udf(col("nutriscore"), col("additives_list")))

# Identifier les produits ultra-transformés (basé sur la présence d'additifs et le Nutri-Score)
enriched_df = enriched_df \
    .withColumn("is_ultra_processed", 
               when((size(col("additives_list")) > 3) | 
                    (col("nutriscore").isin(["d", "e", "D", "E"])), 
                    lit(True)).otherwise(lit(False)))

# Extraction des informations nutritionnelles (si au format JSON)
def extract_nutrient(nutriments_json, nutrient_name):
    if nutriments_json:
        try:
            nutrients = json.loads(nutriments_json)
            return nutrients.get(nutrient_name)
        except:
            return None
    return None

extract_nutrient_udf = udf(extract_nutrient, FloatType())

# Extraction des macronutriments principaux
nutrient_columns = ["energy", "fat", "saturated-fat", "carbohydrates", "sugars", "fiber", "proteins", "salt"]
for nutrient in nutrient_columns:
    enriched_df = enriched_df.withColumn(
        f"{nutrient.replace('-', '_')}_100g", 
        extract_nutrient_udf(col("nutriments"), lit(nutrient))
    )

# Identification des produits bio
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

# Partitionnement et stockage dans BigQuery
# Partitionnement par nutriscore pour optimiser les requêtes d'analyse
enriched_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.processed_food_data") \
    .option("temporaryGcsBucket", "votre-bucket-temporaire") \
    .option("partitionField", "nutriscore") \
    .option("partitionType", "STRING") \
    .mode("overwrite") \
    .save()

# Création d'une vue agrégée par catégorie d'aliments
category_stats = enriched_df \
    .groupBy("categories") \
    .agg(
        {"health_score": "avg", 
         "is_ultra_processed": "sum", 
         "is_bio": "sum", 
         "id": "count"}
    ) \
    .withColumnRenamed("avg(health_score)", "avg_health_score") \
    .withColumnRenamed("sum(is_ultra_processed)", "ultra_processed_count") \
    .withColumnRenamed("sum(is_bio)", "bio_count") \
    .withColumnRenamed("count(id)", "total_products")

# Sauvegarder les statistiques par catégorie
category_stats.write \
    .format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.category_stats") \
    .option("temporaryGcsBucket", "votre-bucket-temporaire") \
    .mode("overwrite") \
    .save()

print("Traitement terminé avec succès!")
