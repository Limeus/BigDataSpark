-- PostgreSQL: raw source and Spark-built warehouse checks.
SELECT 'staging.mock_data' AS object_name, COUNT(*) AS row_count FROM staging.mock_data
UNION ALL SELECT 'dw.dim_country', COUNT(*) FROM dw.dim_country
UNION ALL SELECT 'dw.dim_geo_location', COUNT(*) FROM dw.dim_geo_location
UNION ALL SELECT 'dw.dim_pet', COUNT(*) FROM dw.dim_pet
UNION ALL SELECT 'dw.dim_customer', COUNT(*) FROM dw.dim_customer
UNION ALL SELECT 'dw.dim_seller', COUNT(*) FROM dw.dim_seller
UNION ALL SELECT 'dw.dim_supplier', COUNT(*) FROM dw.dim_supplier
UNION ALL SELECT 'dw.dim_store', COUNT(*) FROM dw.dim_store
UNION ALL SELECT 'dw.dim_product_category', COUNT(*) FROM dw.dim_product_category
UNION ALL SELECT 'dw.dim_product_attribute', COUNT(*) FROM dw.dim_product_attribute
UNION ALL SELECT 'dw.dim_product', COUNT(*) FROM dw.dim_product
UNION ALL SELECT 'dw.dim_date', COUNT(*) FROM dw.dim_date
UNION ALL SELECT 'dw.fact_sales', COUNT(*) FROM dw.fact_sales
ORDER BY object_name;

-- ClickHouse: report tables check.
-- Run in ClickHouse SQL console:
-- SELECT database, table, total_rows
-- FROM system.tables
-- WHERE database = 'lab2' AND table LIKE 'report_%'
-- ORDER BY table;
