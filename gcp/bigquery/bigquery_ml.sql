CREATE OR REPLACE TABLE `analytics-trafic-idfm.food_consumption_trends.ml_features` AS
SELECT
  id,
  product_name,
  -- categories,
  IFNULL(energy_100g, 0) AS energy_100g,
  IFNULL(fat_100g, 0) AS fat_100g,
  IFNULL(saturated_fat_100g, 0) AS saturated_fat_100g,
  IFNULL(carbohydrates_100g, 0) AS carbohydrates_100g,
  IFNULL(sugars_100g, 0) AS sugars_100g,
  IFNULL(fiber_100g, 0) AS fiber_100g,
  IFNULL(proteins_100g, 0) AS proteins_100g,
  IFNULL(salt_100g, 0) AS salt_100g,
  health_score,
  ARRAY_LENGTH(additives_list) AS additives_count,
  nutriscore,
  CASE 
    WHEN nutriscore = 'a' THEN 5
    WHEN nutriscore = 'b' THEN 4
    WHEN nutriscore = 'c' THEN 3
    WHEN nutriscore = 'd' THEN 2
    WHEN nutriscore = 'e' THEN 1
    ELSE 0
  END AS nutriscore_numeric,
  is_bio,
  is_ultra_processed
FROM
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`
-- WHERE
  -- energy_100g != 0
  -- AND fat_100g != 0
  -- AND carbohydrates_100g != 0
  -- AND proteins_100g != 0
;


CREATE OR REPLACE MODEL
  `analytics-trafic-idfm.food_consumption_trends.kmeans_nutrition_model`
OPTIONS
  (model_type='kmeans',
   num_clusters=5) AS
SELECT
  energy_100g,
  fat_100g,
  saturated_fat_100g,
  carbohydrates_100g,
  sugars_100g,
  fiber_100g,
  proteins_100g,
  salt_100g,
  -- health_score,
  additives_count,
  -- nutriscore_numeric
FROM
  `analytics-trafic-idfm.food_consumption_trends.ml_features`
;


SELECT *
FROM ML.EVALUATE(MODEL `analytics-trafic-idfm.food_consumption_trends.kmeans_nutrition_model`);


SELECT
  centroid_id AS cluster_id,
  ROUND(AVG(CASE WHEN feature = 'energy_100g' THEN numerical_value END), 2) AS avg_energy,
  ROUND(AVG(CASE WHEN feature = 'fat_100g' THEN numerical_value END), 2) AS avg_fat,
  ROUND(AVG(CASE WHEN feature = 'saturated_fat_100g' THEN numerical_value END), 2) AS avg_saturated_fat,
  ROUND(AVG(CASE WHEN feature = 'carbohydrates_100g' THEN numerical_value END), 2) AS avg_carbohydrates,
  ROUND(AVG(CASE WHEN feature = 'sugars_100g' THEN numerical_value END), 2) AS avg_sugars,
  ROUND(AVG(CASE WHEN feature = 'fiber_100g' THEN numerical_value END), 2) AS avg_fiber,
  ROUND(AVG(CASE WHEN feature = 'proteins_100g' THEN numerical_value END), 2) AS avg_proteins,
  ROUND(AVG(CASE WHEN feature = 'salt_100g' THEN numerical_value END), 2) AS avg_salt,
  ROUND(AVG(CASE WHEN feature = 'additives_count' THEN numerical_value END), 2) AS avg_additives_count
FROM 
  ML.CENTROIDS(MODEL `analytics-trafic-idfm.food_consumption_trends.kmeans_nutrition_model`)
GROUP BY cluster_id
ORDER BY cluster_id
;


CREATE OR REPLACE TABLE `analytics-trafic-idfm.food_consumption_trends.product_clusters` AS
SELECT 
  f.id,
  f.product_name,
  predictions.centroid_id AS cluster,
  f.nutriscore
FROM 
  `analytics-trafic-idfm.food_consumption_trends.ml_features` f,
  ML.PREDICT(MODEL `analytics-trafic-idfm.food_consumption_trends.kmeans_nutrition_model`, 
    (SELECT 
      id as sub_id,
      energy_100g,
      fat_100g,
      saturated_fat_100g,
      carbohydrates_100g,
      sugars_100g,
      fiber_100g,
      proteins_100g,
      salt_100g,
      additives_count
    FROM 
      `analytics-trafic-idfm.food_consumption_trends.ml_features`)) AS predictions
WHERE f.id = predictions.sub_id
;



