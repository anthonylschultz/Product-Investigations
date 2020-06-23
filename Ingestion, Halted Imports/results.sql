-- 1. customer impact
-- Halted Imports
-- 1. Customer impact
--     - Customer impact -  monthly
--     - Customer SLA violations because of halted imports 
--     - Customer churn risk?
--     - Revenue/customer?
--     - Those who have halted imports, do they send files via SFTP or Connect? 
-- 2. TO impact
--     - Time: 230 canaries/week - 138 ingestion-related. 12 imports/canary = 1,476 imports/week. 2 minutes/import = 2,214 minutes/week = 37 hours/week
--     - Time: 230 canaries/week - 138 ingestion-related, 12 imports/canary = 1,656 imports/week. 5 minutes/import = 8.280 minutes/week = 207 hours/week
--     - 18 TOs, 3 leads
--     - 25% of TOs time on manual processes
--     - $$$: 
-- 3. Reasons a file may get halted in pre-processing:
--     - different field casing
--     - identifiers not mapped
--     - no audience key
--     - new field in file
--     - always halt
--     - no fields
--     - field type changes enum -> raw
--     - columns configured that contain unique identifiers
-- 4. Repeat offenders? monthly table pulls
-- 5. Simple onboard vs. SFTP
--     - customers who SFTP upload vs UI
-- 6. sftp, connect
-- 7. Hackweek project
-- -----------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Customer impact
--     - Customer impact - total, monthly

-- total
SELECT COUNT(LIR.id) AS number_halted_imports, LCA.customer_id
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
    ON LIR.liveramp_customer_account_id = LCA.id
WHERE status = 3 
GROUP BY LCA.customer_id
ORDER BY number_halted_imports DESC

SELECT *
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` WHERE DATE(updated_at) = "2020-02-03" 

-- monthly


-- for Sneha - monthly lookback from ~61 customers
--- create 
SELECT updated_at
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` 
WHERE (DATE(updated_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE()) 
ORDER BY updated_at ASC


SELECT LCA.customer_id, LIR.updated_at
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
    ON LIR.liveramp_customer_account_id = LCA.id
WHERE LCA.customer_id IN (SELECT Customer_ID FROM `liveramp-ts-bigquery.aschultz.20200204_ingestion_customer_ids`) AND (DATE(LIR.updated_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE())
GROUP BY LCA.customer_id, LIR.updated_at

SELECT LCA.customer_id, LIR.updated_at
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
    ON LIR.liveramp_customer_account_id = LCA.id
WHERE LCA.customer_id IN (SELECT Customer_ID FROM `liveramp-ts-bigquery.aschultz.20200204_ingestion_customer_ids`) AND (DATE(LIR.updated_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE())


--- DATE - LAST 30 DAYS
((LIR.updated_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE())
GROUP BY LCA.customer_id, LIR.updated_at
--- DATE_DIFF - LAST 30 DAYS
WHERE LCA.customer_id IN (SELECT Customer_ID FROM `liveramp-ts-bigquery.aschultz.20200204_ingestion_customer_ids`) AND 

---
SELECT LCA.customer_id, LIR.updated_at
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
    ON LIR.liveramp_customer_account_id = LCA.id
WHERE LCA.customer_id IN (SELECT Customer_ID FROM `liveramp-ts-bigquery.aschultz.20200204_ingestion_customer_ids`) AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), LIR.updated_at, day) <= 30 AND LIR.status = 3
ORDER BY LIR.updated_at ASC
---



--     - Customer SLA violations because of halted imports 
--     - Revenue/customer


SELECT *
FROM `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
  INNER JOIN `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
    ON LIR.liveramp_customer_account_id = LCA.id
WHERE LCA.customer_id = 538729
