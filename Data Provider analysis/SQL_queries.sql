/* 
1) 
Data Provider SLAs by volume and revenue - Data usage patterns by data provider
Customer ID | files over 500m rows | total customer files 
*/ 
SELECT INGEST_SLA.cust_id, COUNT(*) AS files_over_500m
FROM `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
WHERE (SELECT CUST_AUD.id 
              FROM `liveramp-ts-bigquery.aschultz.DP_pull_customers_audiences_audiencemembers` 
              AS CUST_AUD) AND INGEST_SLA.num_records > 500000000
GROUP BY INGEST_SLA.cust_id
ORDER BY files_over_500m DESC

/* 
2) 
Data Provider Cust IDs 
*/
SELECT INGEST_SLA.cust_id
FROM
  `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
WHERE INGEST_SLA.cust_id IN (SELECT customer_id 
              FROM `liveramp-ts-bigquery.aschultz.DataSellerCustomerIDs`)  
              AND INGEST_SLA.AIR_createdat IS NOT NULL
GROUP BY INGEST_SLA.cust_id

/* 
3) 
frequency - per quarter/month/etc. - single customer, multiple customers
*/
WITH air_timestamp AS (
  SELECT INGEST_SLA.cust_id, CAST(TIMESTAMP_TRUNC(air_createdat, month) as date) AS month, COUNT(air_id) AS num_requests, SUM(num_files) AS sum_total_files, SUM(INGEST_SLA.num_records) AS sum_total_records
  FROM `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
  WHERE INGEST_SLA.cust_id IN (SELECT customer_id 
                FROM `liveramp-ts-bigquery.aschultz.DataSellerCustomerIDs`)  
                AND INGEST_SLA.AIR_createdat IS NOT NULL
  GROUP BY INGEST_SLA.cust_id, month
  )
SELECT cust_id, AVG(num_requests) AS avg_num_AIR_requests, AVG(sum_total_files) AS avg_files_per_month, MAX(month) AS max, MIN(month) AS min, DATE_DIFF(MAX(month), MIN(month), month) + 1 AS cust_lifetime_duration, AVG(sum_total_records) AS average_records_per_AIR
FROM air_timestamp
GROUP BY cust_id

/* 
4) 
Ingestion Stats by DP, count imports, files per import 
*/
SELECT
  RLDBCUST.name,
  INGEST_SLA.cust_id,
  COUNT(INGEST_SLA.lir_id) AS count_imports,
  SUM(INGEST_SLA.num_files) AS num_files,
  SUM(INGEST_SLA.total_file_sizes)/(1024*1024*1024) AS total_size_in_gb,
  SUM(num_records) AS total_records,
  SUM(import_field_count) AS num_fields_multipled_by_import,
  AVG(INGEST_SLA.num_files) AS avg_num_files_per_import
FROM
  `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
  INNER JOIN `corp-bi-us-prod.rldb.customers` AS RLDBCUST
    ON INGEST_SLA.cust_id = RLDBCUST.id
WHERE INGEST_SLA.cust_id in (SELECT customer_id FROM `liveramp-ts-bigquery.aschultz.DataSellerCustomerIDs`)
  AND INGEST_SLA.data_entry_time > "2019-01-01"
GROUP BY 1, 2
                                                                                                                                      
/* 
5) 
Time Series of imports, grouped by customer id + week 
*/
SELECT
  INGEST_SLA.cust_id,
  FORMAT_TIMESTAMP('%Y-%U', INGEST_SLA.air_createdat) AS week,
  COUNT(INGEST_SLA.num_files) AS num_files_over_500m_records,
FROM
  `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
WHERE
  INGEST_SLA.num_records > 500000000
  AND INGEST_SLA.AIR_createdat IS NOT NULL
GROUP BY
  week,
  INGEST_SLA.cust_id
ORDER BY
  cust_id ASC,
  week DESC

/*
6) 
Cust ID | # of files over 500m | total num of files
*/
SELECT INGEST_SLA.cust_id, COUNT(*) AS files_over_500m, SUM(INGEST_SLA.num_files) AS total_customer_files
FROM `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
WHERE EXISTS (SELECT CUST_AUD.id 
              FROM `liveramp-ts-bigquery.aschultz.DP_pull_customers_audiences_audiencemembers` 
              AS CUST_AUD) AND INGEST_SLA.num_records > 500000000
GROUP BY INGEST_SLA.cust_id
ORDER BY files_over_500m DESC
