
-- Product distribution by nutriscore
SELECT 
  nutriscore,
  COUNT(*) as product_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `analytics-trafic-idfm.food_consumption_trends.processed_data_off`), 2) AS percentage
FROM 
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`
WHERE 
  nutriscore IS NOT NULL
GROUP BY 
  nutriscore
;


-- Distribution of ultra-processed products by category
SELECT 
  categories,
  COUNT(*) as total_products,
  SUM(CASE WHEN is_ultra_processed = true THEN 1 ELSE 0 END) as ultra_processed_count,
  ROUND(SUM(CASE WHEN is_ultra_processed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ultra_processed_percentage
FROM 
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`
WHERE categories IS NOT NULL
  AND TRIM(categories) NOT IN ('null', 'undefined', 'unknown', '')
GROUP BY 
  categories
ORDER BY 
  ultra_processed_count DESC
;



-- The most addictive
SELECT 
  SPLIT(additive, ':')[OFFSET(1)] as additive,
  COUNT(*) as product_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `analytics-trafic-idfm.food_consumption_trends.processed_data_off`), 2) AS percentage
FROM 
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`,
  UNNEST(additives_list) as additive
GROUP BY 
  additive
ORDER BY 
  product_count DESC
;


-- Bio vs not bio
SELECT 
  CASE 
    WHEN is_bio THEN 'Bio' 
    ELSE 'Non-Bio' 
  END as product_type,
  COUNT(*) as product_count,
  AVG(health_score) as avg_health_score,
  AVG(ARRAY_LENGTH(additives_list)) as avg_additives_count,
  COUNTIF(is_ultra_processed = true) as ultra_processed_count,
  ROUND(COUNTIF(is_ultra_processed = true) * 100.0 / COUNT(*), 2) as ultra_processed_percentage
FROM 
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`
GROUP BY 
  product_type;



-- Nutritional composition by category
SELECT 
  categories,
  COUNT(*) as product_count,
  AVG(health_score) as avg_health_score,
  AVG(energy_100g) as avg_energy,
  AVG(fat_100g) as avg_fat,
  AVG(saturated_fat_100g) as avg_saturated_fat,
  AVG(carbohydrates_100g) as avg_carbs,
  AVG(sugars_100g) as avg_sugars,
  AVG(fiber_100g) as avg_fiber,
  AVG(proteins_100g) as avg_proteins,
  AVG(salt_100g) as avg_salt
FROM 
  `analytics-trafic-idfm.food_consumption_trends.processed_data_off`
WHERE 
  categories IS NOT NULL
GROUP BY 
  categories
ORDER BY 
  avg_health_score DESC;




