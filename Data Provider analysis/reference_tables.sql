/* RLDB <-> Salesforce <-> Workday mapping */
rldb customer ID | Salesforce ID | Workday MID | Workday MID_name 
SELECT CUSTPER.customer_id, RLDB_to_Salesforce.salesforce_account_id, SF_to_Workday.cust_mid, SF_to_Workday.cust_mid_name
FROM `corp-bi-us-prod.rldb.customer_personas` AS CUSTPER
  INNER JOIN `corp-bi-us-prod.rldb.customers` AS CUSTOMERS
    ON CUSTPER.customer_id = CUSTOMERS.id 
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LRCA
    ON CUSTOMERS.id = LRCA.customer_id
  INNER JOIN `corp-bi-us-prod.Mappings.rldb_salesforce` AS RLDB_to_Salesforce
    ON CUSTPER.customer_id = RLDB_to_Salesforce.rldb_customer_id
  INNER JOIN `corp-bi-us-prod.Mappings.workday_salesforce` AS SF_to_Workday
    ON RLDB_to_Salesforce.salesforce_account_id = SF_to_Workday.salesforce_account_id
WHERE CUSTPER.persona_name = "functional_data_seller" AND CUSTOMERS.status = 1 
GROUP BY CUSTPER.customer_id, RLDB_to_Salesforce.salesforce_account_id, SF_to_Workday.cust_mid, SF_to_Workday.cust_mid_name

/*
Customer ID | files over 500m rows | total customer files | salesforce account ID | workday ID
*/
SELECT INGEST_SLA.cust_id, COUNT(*) AS files_over_500m, SUM(INGEST_SLA.num_files) AS total_customer_files, RLDB_SF.salesforce_account_id, workday_SF.cust_mid AS workday_id
FROM `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
INNER JOIN `corp-bi-us-prod.Mappings.rldb_salesforce` AS RLDB_SF
    ON INGEST_SLA.cust_id = rldb_sf.rldb_customer_id
INNER JOIN `corp-bi-us-prod.Mappings.workday_salesforce` AS workday_SF
    ON RLDB_SF.salesforce_account_id = workday_SF.salesforce_account_id
WHERE EXISTS (SELECT CUST_AUD.id 
            FROM `liveramp-ts-bigquery.aschultz.DP_pull_customers_audiences_audiencemembers` 
            AS CUST_AUD) AND INGEST_SLA.num_records > 500000000
GROUP BY INGEST_SLA.cust_id, RLDB_SF.salesforce_account_id, workday_SF.cust_mid
ORDER BY files_over_500m DESC

/* list of Data Provider customer IDs */
Customer ID | audience ID | audience size 
SELECT CP.customer_id, LRC.id, LRC.num_audience_members
FROM `corp-bi-us-prod.rldb.customer_personas` AS CP
  INNER JOIN `corp-bi-us-prod.rldb.customers` AS CUST
    ON CP.customer_id = CUST.id
      INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LRC
        ON LRC.customer_id = CUST.id
WHERE persona_name = "functional_data_seller" OR persona_name = "vertical_paying_platform" OR persona_name = "vertical_brand" AND CUST.status = 1 AND LRC.enabled = TRUE AND LRC.customer_link_audience = FALSE AND LRC.num_audience_members IS NOT NULL

/* ingestion SLA table */
SELECT *
FROM `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
WHERE INGEST_SLA.cust_id = 140601 AND INGEST_SLA.num_records > 500000000

/* customer IDs + names */
SELECT
  RLDBCUST.name,
  INGEST_SLA.cust_id,
  COUNT(INGEST_SLA.num_files) AS num_files_over_500m_records
FROM
  `liveramp-eng-pie.pi_product.ingestion_sla_data` AS INGEST_SLA
  INNER JOIN `corp-bi-us-prod.rldb.customers` AS RLDBCUST
    ON INGEST_SLA.cust_id = RLDBCUST.id
WHERE INGEST_SLA.cust_id in (SELECT customer_id 
              FROM `liveramp-ts-bigquery.aschultz.DataSellerCustomerIDs`) 
              AND INGEST_SLA.num_records > 500000000 
              AND INGEST_SLA.AIR_createdat IS NOT NULL
GROUP BY
  INGEST_SLA.cust_id,
  RLDBCUST.name
ORDER BY
  COUNT(INGEST_SLA.num_files) DESC


