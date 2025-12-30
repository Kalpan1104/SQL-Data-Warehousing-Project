/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

===============================================================================
*/

-- Loading silver_cust_info
INSERT INTO DataWarehouse.silver_crm_cust_info (
    cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,  -- Normalize marital status values to readable format
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,  -- Normalize gender values to readable format
    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM DataWarehouse.bronze_crm_cust_info  
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;  -- Select the most recent record per customer

-- Loading silver_prd_info
INSERT INTO DataWarehouse.silver_crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(NULLIF(TRIM(prd_cost), ''), 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line,
    STR_TO_DATE(
        CASE 
            WHEN LENGTH(TRIM(prd_start_dt)) = 4 THEN CONCAT(TRIM(prd_start_dt), '-01-01')
            WHEN LENGTH(TRIM(prd_start_dt)) = 7 THEN CONCAT(TRIM(prd_start_dt), '-01')
            ELSE TRIM(prd_start_dt)
        END, 
        '%Y-%m-%d'
    ) AS prd_start_dt,
    NULL AS prd_end_dt
FROM DataWarehouse.bronze_prd_info
WHERE prd_start_dt IS NOT NULL;

-- Loading silver_crm_sales_details
insert into DataWarehouse.silver_crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS CHAR(50)) AS DATE)
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS CHAR(50)) AS DATE)
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS CHAR(50)) AS DATE)
END AS sls_due_dt,
case
	when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price)
		then sls_quantity*abs(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case
	when sls_price is null or sls_price<=0
		then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from DataWarehouse.bronze_sales_details;

