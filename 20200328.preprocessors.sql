SELECT file_processor, COUNT(file_processor)
FROM `corp-bi-us-prod.rldb.auto_import_configs` 
GROUP BY file_processor