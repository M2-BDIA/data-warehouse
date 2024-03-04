/*  Affichage des index présents par table  */

SELECT index_name, table_name
FROM user_indexes;


/*  Taille des tables et des index */

SELECT segment_name, SUM(bytes)/1024/1024 AS "Table Size (MB)" 
FROM user_segments 
GROUP BY segment_name;

SELECT SUM(bytes)/1024/1024/1024 AS "Size index (GB)" 
FROM user_segments
WHERE segment_type = 'INDEX';

SELECT SUM(bytes)/1024/1024/1024 AS "Size table (GB)" 
FROM user_segments
WHERE segment_type = 'TABLE';

SELECT SUM(bytes)/1024/1024/1024 AS "Size table and index (GB)" 
FROM user_segments;


/*  Ajout d'index correspondants aux clés primaires   */

CREATE INDEX idx_business_business_id_unique 
ON business("business_id");

CREATE INDEX idx_visit_visit_id_unique 
ON visit("visit_id");

CREATE INDEX idx_review_review_id_unique 
ON review("review_id");

CREATE INDEX idx_consumer_user_id_unique 
ON consumer("user_id");

CREATE INDEX idx_date_detail_date_id_unique 
ON date_detail("date_id");


/*  Ajout d'index sur les attributs de clés étrangères   */

CREATE INDEX idx_visit_business_id
ON visit("business_id");

CREATE INDEX idx_visit_date_id
ON visit("date_id");

CREATE INDEX idx_review_business_id
ON review("business_id");

CREATE INDEX idx_review_date_id
ON review("date_id");

CREATE INDEX idx_review_user_id
ON review("user_id");


/*  Ajout d'index sur des attributs souvents utilisés pour le filtrage ou le regroupement   */

CREATE INDEX idx_business_main_category
ON business("main_category");

CREATE INDEX idx_business_city
ON business("city");

CREATE INDEX idx_business_postal_code
ON business("postal_code");

CREATE INDEX idx_date_year
ON date_detail("year");

CREATE INDEX idx_date_month
ON date_detail("month");
