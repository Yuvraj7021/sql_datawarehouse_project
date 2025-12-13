Data Catalog – Gold Layer
Overview

The Gold Layer represents the business-curated and analytics-ready data in the data warehouse.
It is designed to support reporting, dashboards, and decision-making use cases and follows a star schema approach with dimension and fact tables.

1. gold.dim_customers
Purpose

Stores customer master data enriched with demographic and geographic attributes.
Used for customer segmentation, behavioral analysis, and sales reporting.

Table Structure
Column Name	Data Type	Description
customer_key	INT	Surrogate key uniquely identifying each customer record.
customer_id	INT	Business identifier assigned to each customer.
customer_number	NVARCHAR(50)	Alphanumeric customer reference number.
first_name	NVARCHAR(50)	Customer’s first name.
last_name	NVARCHAR(50)	Customer’s last or family name.
country	NVARCHAR(50)	Country of residence (e.g., Australia).
marital_status	NVARCHAR(50)	Marital status of the customer (e.g., Married, Single).
gender	NVARCHAR(50)	Gender of the customer (e.g., Male, Female, n/a).
birthdate	DATE	Customer date of birth (YYYY-MM-DD).
create_date	DATE	Date when the customer record was created.
2. gold.dim_products
Purpose

Provides product-level attributes used for product performance analysis, category reporting, and inventory insights.

Table Structure
Column Name	Data Type	Description
product_key	INT	Surrogate key uniquely identifying each product.
product_id	INT	Business identifier for the product.
product_number	NVARCHAR(50)	Alphanumeric product code for tracking and inventory.
product_name	NVARCHAR(50)	Descriptive name of the product.
category_id	NVARCHAR(50)	Identifier for the product category.
category	NVARCHAR(50)	High-level product category (e.g., Bikes, Components).
subcategory	NVARCHAR(50)	Detailed product classification within the category.
maintenance_required	NVARCHAR(50)	Indicates if maintenance is required (Yes/No).
cost	INT	Base cost of the product.
product_line	NVARCHAR(50)	Product line or series (e.g., Road, Mountain).
start_date	DATE	Date when the product became available for sale.
3. gold.fact_sales
Purpose

Stores transactional sales data at the lowest level of granularity.
Used for revenue analysis, customer purchasing behavior, and product performance metrics.

Table Structure
Column Name	Data Type	Description
order_number	NVARCHAR(50)	Unique sales order identifier (e.g., SO54496).
product_key	INT	Foreign key referencing gold.dim_products.
customer_key	INT	Foreign key referencing gold.dim_customers.
order_date	DATE	Date when the order was placed.
shipping_date	DATE	Date when the order was shipped.
due_date	DATE	Date when payment was due.
sales_amount	INT	Total sales value for the order line.
quantity	INT	Number of product units sold.
price	INT	Selling price per unit.
Data Modeling Notes

Star schema design optimized for analytical queries

Surrogate keys used in all dimension tables

Fact table references dimensions using foreign keys

Optimized for BI tools such as Power BI, Microsoft Fabric, and SQL analytics
