-- 1. Préparation des données pour le clustering K-Means
CREATE OR REPLACE TABLE `votre-project-id.open_food_facts.ml_features` AS
SELECT
  id,
  product_name,
  categories,
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
  `votre-project-id.open_food_facts.processed_food_data`
WHERE
  energy_100g IS NOT NULL
  AND fat_100g IS NOT NULL
  AND carbohydrates_100g IS NOT NULL
  AND proteins_100g IS NOT NULL;

-- 2. Création du modèle K-Means pour regrouper les produits par profil nutritionnel
CREATE OR REPLACE MODEL
  `votre-project-id.open_food_facts.kmeans_nutrition_model`
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
  health_score,
  additives_count,
  nutriscore_numeric
FROM
  `votre-project-id.open_food_facts.ml_features`;

-- 3. Analyse des clusters générés
SELECT
  cluster_id,
  COUNT(*) AS product_count,
  AVG(energy_100g) AS avg_energy,
  AVG(fat_100g) AS avg_fat,
  AVG(saturated_fat_100g) AS avg_saturated_fat,
  AVG(carbohydrates_100g) AS avg_carbs,
  AVG(sugars_100g) AS avg_sugars,
  AVG(proteins_100g) AS avg_proteins,
  AVG(salt_100g) AS avg_salt,
  AVG(health_score) AS avg_health_score,
  AVG(additives_count) AS avg_additives,
  AVG(nutriscore_numeric) AS avg_nutriscore,
  COUNTIF(is_bio = true) / COUNT(*) AS bio_percentage,
  COUNTIF(is_ultra_processed = true) / COUNT(*) AS ultra_processed_percentage
FROM
  ML.PREDICT(
    MODEL `votre-project-id.open_food_facts.kmeans_nutrition_model`,
    (SELECT * FROM `votre-project-id.open_food_facts.ml_features`)
  )
GROUP BY
  cluster_id
ORDER BY
  product_count DESC;

-- 4. Extraction des exemples de produits représentatifs pour chaque cluster
CREATE OR REPLACE TABLE `votre-project-id.open_food_facts.cluster_examples` AS
SELECT
  p.cluster_id,
  m.product_name,
  m.categories,
  m.energy_100g,
  m.fat_100g,
  m.carbohydrates_100g,
  m.proteins_100g,
  m.health_score,
  m.additives_count,
  m.nutriscore_numeric,
  m.is_bio,
  m.is_ultra_processed,
  p.centroid_distance,
  ROW_NUMBER() OVER (PARTITION BY p.cluster_id ORDER BY p.centroid_distance) AS rank_in_cluster
FROM
  ML.PREDICT(
    MODEL `votre-project-id.open_food_facts.kmeans_nutrition_model`,
    (SELECT * FROM `votre-project-id.open_food_facts.ml_features`)
  ) p
JOIN
  `votre-project-id.open_food_facts.ml_features` m
ON
  p.id = m.id
QUALIFY rank_in_cluster <= 10;  -- Garder les 10 exemples les plus représentatifs par cluster

-- 5. Création d'un modèle de détection d'anomalies avec K-Means
CREATE OR REPLACE MODEL
  `votre-project-id.open_food_facts.anomaly_detection_model`
OPTIONS
  (model_type='kmeans',
   num_clusters=10) AS
SELECT
  energy_100g,
  fat_100g,
  saturated_fat_100g,
  carbohydrates_100g,
  sugars_100g,
  proteins_100g,
  salt_100g
FROM
  `votre-project-id.open_food_facts.ml_features`;

-- 6. Détection de produits avec des valeurs nutritionnelles aberrantes
CREATE OR REPLACE TABLE `votre-project-id.open_food_facts.nutritional_anomalies` AS
SELECT
  m.id,
  m.product_name,
  m.categories,
  m.energy_100g,
  m.fat_100g,
  m.carbohydrates_100g,
  m.proteins_100g,
  m.salt_100g,
  m.health_score,
  m.nutriscore_numeric,
  p.centroid_distance
FROM
  ML.PREDICT(
    MODEL `votre-project-id.open_food_facts.anomaly_detection_model`,
    (SELECT * FROM `votre-project-id.open_food_facts.ml_features`)
  ) p
JOIN
  `votre-project-id.open_food_facts.ml_features` m
ON
  p.id = m.id
ORDER BY
  p.centroid_distance DESC
LIMIT 100;  -- Top 100 des anomalies

-- 7. Vue synthétique d'analyse des tendances
CREATE OR REPLACE VIEW `votre-project-id.open_food_facts.food_trends_analysis` AS
SELECT
  cluster_id,
  AVG(health_score) AS avg_health_score,
  AVG(nutriscore_numeric) AS avg_nutriscore,
  COUNTIF(is_bio = true) / COUNT(*) AS bio_percentage,
  COUNTIF(is_ultra_processed = true) / COUNT(*) AS ultra_processed_percentage,
  AVG(energy_100g) AS avg_energy,
  AVG(fat_100g) AS avg_fat,
  AVG(saturated_fat_100g) AS avg_saturated_fat,
  AVG(carbohydrates_100g) AS avg_carbs,
  AVG(sugars_100g) AS avg_sugars,
  AVG(proteins_100g) AS avg_proteins,
  AVG(salt_100g) AS avg_salt,
  AVG(additives_count) AS avg_additives_count,
  COUNT(*) AS product_count
FROM
  ML.PREDICT(
    MODEL `votre-project-id.open_food_facts.kmeans_nutrition_model`,
    (SELECT * FROM `votre-project-id.open_food_facts.ml_features`)
  )
GROUP BY
  cluster_id
ORDER BY
  avg_health_score DESC;
