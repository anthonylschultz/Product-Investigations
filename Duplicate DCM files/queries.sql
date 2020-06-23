SELECT *
FROM `corp-bi-us-prod.rldb.liveramp_customer_accounts` 
WHERE display_name LIKE "%DCM%" AND enabled = true



SELECT
  LIR.import_name,
  COUNT(LIR.import_name) 
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
GROUP BY
  LIR.import_name
HAVING COUNT(LIR.import_name) > 1


0) What % of DCM files are duplicate?
1) How many total files? 288,048
2) How many duplicate files? 35,181
3) How many impacted audiences 230
4) How many total audiences? 240
5) How many impacted customers? 71

-- FINAL OUTPUT
SELECT `liveramp-ts-bigquery.aschultz.DCM_files_duplicates`.name, `liveramp-ts-bigquery.aschultz.DCM_files_duplicates`.id AS customer_id, sum(count_duplicates) AS num_duplicates, MAX(`liveramp-ts-bigquery.aschultz.DCM_files_all`.count_all) AS total_files, sum(count_duplicates)/MAX(`liveramp-ts-bigquery.aschultz.DCM_files_all`.count_all) *100 AS percent_duplicate
FROM `liveramp-ts-bigquery.aschultz.DCM_files_duplicates` 
INNER JOIN `liveramp-ts-bigquery.aschultz.DCM_files_all`
  ON `liveramp-ts-bigquery.aschultz.DCM_files_duplicates`.id = `liveramp-ts-bigquery.aschultz.DCM_files_all`.id
GROUP BY customer_id, name
ORDER BY sum(count_duplicates) DESC



-- DUPLICATE files by customer_id
SELECT duplicate_files.name, duplicate_files.id, sum(duplicate_files.total_files) AS total_duplicates, total_files.all_files
FROM `liveramp-ts-bigquery.aschultz.DCM_files_duplicates` AS duplicate_files
INNER JOIN `liveramp-ts-bigquery.aschultz.DCM_files_all` AS all_files
  ON duplicate_files.id = all_files.id
GROUP BY id, name
ORDER BY total_duplicates DESC



-- DUPLICATE files by customer_id, import_name
SELECT
    customers.name, customers.id, COUNT(LIR.import_name) AS total_files, LIR.import_name
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY customers.id, customers.name, LIR.import_name
HAVING COUNT(LIR.import_name) > 1




-- TOTAL files by customer_id
SELECT
    customers.name, customers.id, COUNT(LIR.import_name) AS total_files
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY customers.id, customers.name
ORDER BY total_files DESC


SELECT
    COUNT(LIR.import_name), LIR.import_name, customers.id, customers.name
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY customers.id, customers.name, LIR.import_name
HAVING COUNT(LIR.import_name) > 1
ORDER BY customers.id


SELECT
    COUNT(LIR.import_name), LIR.import_name
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY LIR.import_name
HAVING COUNT(LIR.import_name) > 1


final
SELECT
    LIR.import_name, COUNT(LIR.import_name) AS file_count, LCA.id, LCA.display_name
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY LCA.id, LCA.display_name, LIR.import_name
HAVING file_count > 1




SELECT
    LIR.import_name, COUNT(LIR.import_name) AS file_count
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY LIR.import_name
HAVING file_count > 1

SUM(file_count) IF file_count > 1





SELECT
  customers.id AS cust_id, COUNT(LIR.import_name) AS file_count, LCA.id
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY customers.id, LCA.id, LIR.import_name
HAVING file_count > 1



SELECT
    COUNT(LIR.import_name), LIR.import_name, LCA.id
FROM
  `corp-bi-us-prod.rldb.liveramp_customer_accounts` AS LCA
INNER JOIN
  `corp-bi-us-prod.rldb.liveramp_import_requests` AS LIR
ON
  LCA.id = LIR.liveramp_customer_account_id
INNER JOIN `corp-bi-us-prod.rldb.customers` AS customers
ON LCA.customer_id = customers.id
WHERE
  LCA.display_name LIKE "%DCM%"
  AND LCA.enabled = TRUE
  AND LCA.customer_link_audience = TRUE
  AND customers.status = 1
GROUP BY LCA.id, LIR.import_name
HAVING COUNT(LIR.import_name) > 1

