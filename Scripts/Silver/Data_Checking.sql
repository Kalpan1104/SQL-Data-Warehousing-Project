/* bronze_crm_cust_info*/
# Finding Duplicate ID
select cst_id,count(*) as counting
from DataWarehouse.bronze_crm_cust_info
group by cst_id
having counting>1;

# Finding all first and last names with spaces in either before or after the name
select cst_firstname from DataWarehouse.bronze_crm_cust_info
where cst_firstname!=trim(cst_firstname);

select cst_lastname from DataWarehouse.bronze_crm_cust_info
where cst_lastname!=trim(cst_lastname);

select cst_gndr from DataWarehouse.bronze_crm_cust_info
where cst_gndr!=trim(cst_gndr);  # This shows that gender does not have extra spaces

# Checking all distinct values in gender
select distinct cst_gndr
from DataWarehouse.bronze_crm_cust_info;

# Checking all distinct values in Maritial Status
select distinct cst_marital_status
from DataWarehouse.bronze_crm_cust_info;


/* bronze_prd_info*/
# Finding Duplicate ID
select prd_id,count(*) as counting
from DataWarehouse.bronze_prd_info
group by prd_id
having counting>1; -- No duplicate product id

# Finding all unwanted spaces in product name
select prd_nm from DataWarehouse.bronze_prd_info
where prd_nm!=trim(prd_nm);  -- No unwanted Spaces

# Check invalid dates where start date is after end date
select * from DataWarehouse.bronze_prd_info where prd_end_dt<prd_start_dt
