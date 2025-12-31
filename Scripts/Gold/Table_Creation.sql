/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

create view DataWarehouse.dim_customers as 
select
	row_number() over (order by ci.cst_id) as customer_key,
	ci.cst_id as customer_ID,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
	la.cntry as country,
    ci.cst_marital_status as martial_status,
    case 
		when ci.cst_gndr='n/a' then ci.cst_gndr
        else coalesce(ca.gen,'n/a')
	end as gender,
    ca.bdate as birth_date,
    ci.cst_create_date as create_date
from DataWarehouse.silver_crm_cust_info ci
left join DataWarehouse.silver_erp_cust_az12 ca
on ci.cst_key=ca.cid
left join DataWarehouse.silver_erp_loc_a101 la
on ci.cst_key=la.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

create view DataWarehouse.dim_products as
select
	row_number() over (order by pi.prd_start_dt,pi.prd_key) as product_key,
	pi.prd_id as product_id,
	pi.prd_key as product_number,
	pi.prd_nm as product_name,
  pi.cat_id as category_id,
  ca.cat as category,
  ca.subcat as sub_category,
  ca.maintenance  as maintenance,
	pi.prd_cost as cost,
	pi.prd_line as product_line,
	pi.prd_start_dt as start_date
from DataWarehouse.silver_crm_prd_info pi
left join DataWarehouse.silver_erp_px_cat_g1v2 ca
on pi.cat_id=ca.id
where pi.prd_end_dt is null

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

CREATE VIEW DataWarehouse.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM DataWarehouse.silver_crm_sales_details sd
LEFT JOIN DataWarehouse.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN DataWarehouse.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
