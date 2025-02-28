-- 1. Analyse de la distribution des produits par Nutri-Score
SELECT 
  nutriscore,
  COUNT(*) as product_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `votre-project-id.open_food_facts.processed_food_data`), 2) AS percentage
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  nutriscore IS NOT NULL
GROUP BY 
  nutriscore
ORDER BY 
  CASE 
    WHEN nutriscore = 'a' THEN 1 
    WHEN nutriscore = 'b' THEN 2 
    WHEN nutriscore = 'c' THEN 3 
    WHEN nutriscore = 'd' THEN 4 
    WHEN nutriscore = 'e' THEN 5 
    ELSE 6 
  END;

-- 2. Évolution des produits ultra-transformés par catégorie
SELECT 
  categories,
  COUNT(*) as total_products,
  SUM(CASE WHEN is_ultra_processed = true THEN 1 ELSE 0 END) as ultra_processed_count,
  ROUND(SUM(CASE WHEN is_ultra_processed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ultra_processed_percentage
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  categories IS NOT NULL
GROUP BY 
  categories
HAVING 
  COUNT(*) > 10  -- Filtre pour les catégories avec suffisamment de produits
ORDER BY 
  ultra_processed_percentage DESC
LIMIT 20;

-- 3. Top 10 des additifs les plus courants
SELECT 
  additive,
  COUNT(*) as product_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `votre-project-id.open_food_facts.processed_food_data`), 2) AS percentage
FROM 
  `votre-project-id.open_food_facts.processed_food_data`,
  UNNEST(additives_list) as additive
GROUP BY 
  additive
ORDER BY 
  product_count DESC
LIMIT 10;

-- 4. Relation entre Nutri-Score et nombre d'additifs
SELECT 
  nutriscore,
  AVG(ARRAY_LENGTH(additives_list)) as avg_additives_count,
  STDDEV(ARRAY_LENGTH(additives_list)) as stddev_additives_count,
  MIN(ARRAY_LENGTH(additives_list)) as min_additives_count,
  MAX(ARRAY_LENGTH(additives_list)) as max_additives_count
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  nutriscore IS NOT NULL
GROUP BY 
  nutriscore
ORDER BY 
  CASE 
    WHEN nutriscore = 'a' THEN 1 
    WHEN nutriscore = 'b' THEN 2 
    WHEN nutriscore = 'c' THEN 3 
    WHEN nutriscore = 'd' THEN 4 
    WHEN nutriscore = 'e' THEN 5 
    ELSE 6 
  END;

-- 5. Analyse des produits bio vs non-bio
SELECT 
  'Bio' as product_type,
  COUNT(*) as product_count,
  AVG(health_score) as avg_health_score,
  AVG(ARRAY_LENGTH(additives_list)) as avg_additives_count,
  COUNTIF(is_ultra_processed = true) as ultra_processed_count,
  ROUND(COUNTIF(is_ultra_processed = true) * 100.0 / COUNT(*), 2) as ultra_processed_percentage
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  is_bio = true
UNION ALL
SELECT 
  'Non-Bio' as product_type,
  COUNT(*) as product_count,
  AVG(health_score) as avg_health_score,
  AVG(ARRAY_LENGTH(additives_list)) as avg_additives_count,
  COUNTIF(is_ultra_processed = true) as ultra_processed_count,
  ROUND(COUNTIF(is_ultra_processed = true) * 100.0 / COUNT(*), 2) as ultra_processed_percentage
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  is_bio = false;

-- 6. Comparaison nutritionnelle par type de produit
SELECT 
  product_type,
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
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  product_type IS NOT NULL
GROUP BY 
  product_type
HAVING 
  COUNT(*) > 5
ORDER BY 
  avg_health_score DESC;

-- 7. Analyse des écoscores par catégorie (si disponible)
SELECT 
  categories,
  COUNT(*) as product_count,
  AVG(ecoscore_score) as avg_ecoscore,
  COUNTIF(ecoscore_grade = 'a') as ecoscore_a_count,
  COUNTIF(ecoscore_grade = 'b') as ecoscore_b_count,
  COUNTIF(ecoscore_grade = 'c') as ecoscore_c_count,
  COUNTIF(ecoscore_grade = 'd') as ecoscore_d_count,
  COUNTIF(ecoscore_grade = 'e') as ecoscore_e_count
FROM 
  `votre-project-id.open_food_facts.processed_food_data`
WHERE 
  categories IS NOT NULL 
  AND ecoscore_score IS NOT NULL
GROUP BY 
  categories
HAVING 
  COUNT(*) > 10
ORDER BY 
  avg_ecoscore DESC
LIMIT 20;
