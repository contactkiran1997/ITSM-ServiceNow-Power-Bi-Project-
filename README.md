# End-to-End Microsoft Fabric Medallion Lakehouse & Warehouse Project

## 📌 Project Overview
This project implements an end-to-end Enterprise Data Analytics platform using **Microsoft Fabric**. It leverages the **Medallion Architecture (Bronze → Silver → Gold)** to ingest raw multi-source transactional data from GitHub via Fabric Data Pipelines, transform it into cleansed Delta tables using Fabric PySpark Notebooks, and expose aggregated analytical models via a Fabric Synapse Data Warehouse and Power BI DirectLake semantic model.

---

## 🏗 Architecture Diagram
```text
<img width="1242" height="302" alt="image" src="https://github.com/user-attachments/assets/266299b2-042e-46be-aa7f-2d25ae66796f" />


[ GitHub Raw Data (CSV/JSON) ]
             │
             ▼ (Fabric Data Factory: Copy Activity)
┌──────────────────────────────────────────────────────────┐
│                   Microsoft Fabric OneLake               │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ 🥉 Bronze Layer (Lakehouse / Raw Files & Tables)   │  │
│  │    - Raw ingested files (Immutable data store)      │  │
│  └────────────────────────┬───────────────────────────┘  │
│                           │ (PySpark Notebook)           │
│                           ▼                              │
│  ┌────────────────────────────────────────────────────┐  │
│  │ 🥈 Silver Layer (Lakehouse / Cleansed Delta)       │  │
│  │    - Deduplication, Schema Enforcement, Cleansing  │  │
│  └────────────────────────┬───────────────────────────┘  │
│                           │ (PySpark Notebook)           │
│                           ▼                              │
│  ┌────────────────────────────────────────────────────┐  │
│  │ 🥇 Gold Layer (Lakehouse / Star Schema Delta)      │  │
│  │    - Fact & Dimension Tables, Aggregations         │  │
│  └────────────────────────┬───────────────────────────┘  │
└───────────────────────────┼──────────────────────────────┘
                            │ (T-SQL / Pipeline Orchestration)
                            ▼
┌──────────────────────────────────────────────────────────┐
│      🏢 Fabric Synapse Data Warehouse (Analytical Layer) │
│       - Star Schema / Dimensional Model                  │
│       - Row-Level Security (RLS) & Granular Access       │
│       - Cross-Database T-SQL Query Support               │
└───────────────────────────┬──────────────────────────────┘
                            │ (DirectLake Mode)
                            ▼
┌──────────────────────────────────────────────────────────┐
│              📊 Power BI Executive Dashboards            │
│       - High-performance reporting, DAX KPIs, Trends     │
└──────────────────────────────────────────────────────────┘

🛠 Tech Stack & Tools
Platform: Microsoft Fabric, OneLake

Orchestration: Fabric Data Factory (Data Pipelines, Copy Activity)

Compute / Processing: Apache Spark, PySpark Notebooks

Storage Format: Delta Lake (.delta Parquet)

Serving Layer: Fabric Synapse Data Warehouse (T-SQL)

Reporting: Power BI (DirectLake Mode, Semantic Modeling, Advanced DAX)

Security & Governance: Row-Level Security (RLS), Delta Table constraints

🚀 Step-by-Step Implementation Guide
Phase 1: Workspace & Lakehouse Setup
Log in to Microsoft Fabric.

Create Workspace:

Navigate to Workspaces > New Workspace.

Name: ws-enterprise-analytics-prod.

Under Advanced, assign a dedicated Fabric Capacity (Trial, F-SKU, or P-SKU).

Create Lakehouse:

In the workspace, select + New > Lakehouse.

Name: lh_enterprise_medallion.

Enable schema support and verify the created Files and Tables directories inside OneLake.

Phase 2: Ingestion via Data Factory Pipeline (Bronze Layer)
Create Data Pipeline:

Select + New > Data Pipeline > Name: pl_ingest_github_to_bronze.

Configure Copy Data Activity:

Add a Copy data activity to the canvas.

Source Configuration:

Connection Type: HTTP / Web.

Base URL: https://raw.githubusercontent.com/<username>/<repo>/main/data/

Relative URL: raw_sales_data.csv

Request Method: GET

Format: DelimitedText (CSV).

Destination / Sink Configuration:

Workspace: ws-enterprise-analytics-prod

Store Type: Lakehouse (lh_enterprise_medallion)

Root Folder: Files

Directory: bronze/sales/

File Name: sales_raw.csv

Run & Validate: Trigger the pipeline and ensure the raw file lands in OneLake/Files/bronze/.

Phase 3: Data Transformation via PySpark Notebooks
1. Ingest Bronze to Raw Delta Table
Python
# Notebook: 01_Bronze_Ingestion
from pyspark.sql.types import *

bronze_file_path = "Files/bronze/sales/sales_raw.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_file_path)

# Write to Bronze Delta Table (Raw & Immutable)
df_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_sales_raw")
display(spark.table("bronze_sales_raw").limit(5))
2. Process Bronze to Cleansed Silver Layer
Python
# Notebook: 02_Silver_Cleansing
from pyspark.sql.functions import col, to_date, trim, current_timestamp

df_bronze = spark.read.table("bronze_sales_raw")

# Data Hygiene & Type Casting
df_silver = df_bronze \
    .filter(col("TransactionID").isNotNull()) \
    .dropDuplicates(["TransactionID"]) \
    .withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd")) \
    .withColumn("CustomerID", col("CustomerID").cast("integer")) \
    .withColumn("Amount", col("Amount").cast("decimal(18,2)")) \
    .withColumn("Region", trim(col("Region"))) \
    .withColumn("IngestionTimestamp", current_timestamp())

# Write to Silver Delta Table
df_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("silver_sales_cleansed")
3. Build Dimensional Gold Layer (Star Schema)
Python
# Notebook: 03_Gold_Dimensional_Modeling
from pyspark.sql.functions import monotonically_increasing_id, col, sum, count, avg

df_silver = spark.read.table("silver_sales_cleansed")

# 1. Dimension: DimCustomer
dim_customer = df_silver.select("CustomerID", "CustomerName", "CustomerEmail", "Region") \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("CustomerKey", monotonically_increasing_id())

dim_customer.write.mode("overwrite").format("delta").saveAsTable("gold_dim_customer")

# 2. Fact: FactSales
fact_sales = df_silver.select(
    col("TransactionID").alias("SalesOrderNumber"),
    col("TransactionDate"),
    col("CustomerID"),
    col("ProductID"),
    col("Quantity"),
    col("Amount").alias("SalesAmount")
)

fact_sales.write.mode("overwrite").format("delta").saveAsTable("gold_fact_sales")
Phase 4: Serving to Synapse Data Warehouse
For enterprise analytical workloads requiring complex cross-database T-SQL joins and strict role-based access governance:

Create Fabric Synapse Warehouse:

Go to Workspace > + New > Warehouse > Name: wh_enterprise_analytics.

Move & Model Data via T-SQL / Cross-Database Querying:

SQL
-- Create Schemas
CREATE SCHEMA Gold;
GO

-- Load Dimension Table from Lakehouse into Warehouse
CREATE TABLE Gold.DimCustomer AS
SELECT 
    CustomerKey,
    CustomerID,
    CustomerName,
    CustomerEmail,
    Region
FROM [lh_enterprise_medallion].[dbo].[gold_dim_customer];
GO

-- Load Fact Table
CREATE TABLE Gold.FactSales AS
SELECT 
    SalesOrderNumber,
    TransactionDate,
    CustomerID,
    ProductID,
    Quantity,
    SalesAmount
FROM [lh_enterprise_medallion].[dbo].[gold_fact_sales];
GO

-- Create Analytical KPI View
CREATE VIEW Gold.vw_RegionalPerformance AS
SELECT 
    c.Region,
    YEAR(f.TransactionDate) AS SalesYear,
    MONTH(f.TransactionDate) AS SalesMonth,
    SUM(f.SalesAmount) AS TotalRevenue,
    COUNT(DISTINCT f.SalesOrderNumber) AS TotalOrders
FROM Gold.FactSales f
JOIN Gold.DimCustomer c ON f.CustomerID = c.CustomerID
GROUP BY c.Region, YEAR(f.TransactionDate), MONTH(f.TransactionDate);
GO
Phase 5: Power BI Semantic Modeling & DirectLake Delivery
DirectLake Mode Semantic Model:

Open the Warehouse or Lakehouse SQL Analytics Endpoint.

Navigate to Reporting > New semantic model.

Select gold_fact_sales and related dimensions (gold_dim_customer, gold_dim_date, gold_dim_product).

Establish Relationships:

gold_dim_customer[CustomerID] (1) ───< gold_fact_sales[CustomerID] (*)

Core DAX KPI Measures:

Code snippet
Total Sales = SUM(gold_fact_sales[SalesAmount])

YoY Sales Growth = 
VAR CurrentSales = [Total Sales]
VAR PreviousSales = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(gold_dim_date[Date]))
RETURN
    DIVIDE(CurrentSales - PreviousSales, PreviousSales, 0)
Security & Governance:

Configured Dynamic Row-Level Security (RLS) filtering by user email using [UserEmail] = USERPRINCIPALNAME().

📈 Business Impact & Key Takeaways
Unified Governance: Lakehouse data is instantly queryable across both Spark notebooks and T-SQL Synapse Warehouses via OneLake without data movement.

DirectLake Performance: Power BI connects directly to Delta Parquet files in OneLake, bypassing Import refresh limits and DirectQuery latency.

Scalable ETL: Pipeline orchestration combined with Delta Lake ACID transactions ensures 99.9% data reliability and automated daily refreshes.

👤 Author
Kiran Chaudhari – Senior Power BI Developer & Data Analyst

LinkedIn: linkedin.com/in/kiran-chaudhari

Email: contact.kiran1997@gmail.com
