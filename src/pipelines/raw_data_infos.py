from pyspark.sql.types import *

# OFF = Open Food Fact

DATA_KEYS_OFF = ["id", "_keywords", "allergens_from_ingredients", "brands", "categories", "categories_hierarchy",
            "countries", "countries_imported", "ecoscore_grade", "ecoscore_score", "generic_name_fr", "ingredients", 
            "ingredients_hierarchy", "link", "nutriments", "nutriscore_grade", "nutriscore_version", "nutrition_grade_fr", 
            "origins", "packaging_hierarchy", "product_name", "product_name_fr", "product_type", "additives_tags"]


INGREDIENTS_INFOS_KEYS = ["id", "text", "vegan", "vegetarian"]


INGREDIENTS_SCHEMA = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("vegan", StringType(), True),
    StructField("vegetarian", StringType(), True)
]))


RAW_DATA_SCHEMA_OFF = StructType([
        StructField("id", StringType(), False),
        StructField("_keywords", ArrayType(StringType()), True),
        StructField("allergens_from_ingredients", StringType(), True),
        StructField("brands", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("categories_hierarchy", ArrayType(StringType()), True),
        StructField("countries", StringType(), True),
        StructField("countries_imported", StringType(), True),
        StructField("ecoscore_grade", StringType(), True),
        StructField("ecoscore_score", IntegerType(), True),
        StructField("generic_name_fr", StringType(), True),
        StructField("ingredients", INGREDIENTS_SCHEMA, True),
        StructField("ingredients_hierarchy", ArrayType(StringType()), True),
        StructField("link", StringType(), True),
        StructField("nutriments", StringType(), True),
        StructField("nutriscore_grade", StringType(), True),
        StructField("nutriscore_version", StringType(), True),
        StructField("nutrition_grade_fr", StringType(), True),
        StructField("origins", StringType(), True),
        StructField("packaging_hierarchy", ArrayType(StringType()), True),
        StructField("product_name", StringType(), True),
        StructField("product_name_fr", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("additives_tags", ArrayType(StringType()), True)
    ])


RAW_DATA_ARRAY_FIELDS = ["_keywords", "categories_hierarchy", "ingredients", "ingredients_hierarchy", "packaging_hierarchy", "additives_tags"]