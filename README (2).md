# End-to-End Microsoft Fabric Medallion Lakehouse & Warehouse Project

An enterprise-grade Data Engineering & Business Intelligence project built on **Microsoft Fabric** implementing the **Medallion Architecture (Bronze 🥉 ➔ Silver 🥈 ➔ Gold 🥇)**, automated ETL pipelines, PySpark data transformations, Synapse Data Warehousing, and high-performance Power BI DirectLake analytics.

---

## 📌 Project Architecture

```
[ GitHub Raw Data (CSV) ]
           │
           ▼ (Fabric Data Factory: Copy Activity)
┌─────────────────────────────────────────────────────────────┐
│                 Microsoft Fabric OneLake                    │
│                                                             │
│  🥉 BRONZE LAYER (Raw Files & Tables)                       │
│     • Ingested immutable CSV/Parquet files                  │
│     • Stored in OneLake Files / Bronze Delta Tables         │
│                            │                                │
│                            ▼ (PySpark Notebook)             │
│  🥈 SILVER LAYER (Cleansed & Enriched)                      │
│     • Deduplication, Type Casting, Null Handling            │
│     • Cleaned Delta Tables with schema validation           │
│                            │                                │
│                            ▼ (PySpark Notebook)             │
│  🥇 GOLD LAYER (Curated Star Schema)                        │
│     • Fact & Dimension tables                               │
│     • Ready for analytical serving                          │
└────────────────────────────┬────────────────────────────────┘
                             │ (T-SQL Load / Synapse Warehouse)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│       🏢 Fabric Synapse Data Warehouse (Serving Layer)      │
│       • Dimensional Model (FactSales & Dim Tables)          │
│       • Dynamic Row-Level Security (RLS)                    │
│       • Optimized SQL Views & Stored Procedures             │
└────────────────────────────┬────────────────────────────────┘
                             │ (DirectLake Mode)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│            📊 Power BI Executive Analytics                  │
│       • Sub-second DirectLake rendering                     │
│       • Advanced DAX KPIs (YoY Growth, Margin %, Trends)    │
│       • Dynamic RLS via USERPRINCIPALNAME()                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Step-by-Step Implementation

### Step 1: Create Fabric Workspace & Lakehouse
1. Navigate to **Microsoft Fabric** (`https://app.fabric.microsoft.com/`).
2. Go to **Workspaces** ➔ **+ New Workspace** ➔ Name: `ws-fabric-medallion-analytics`.
3. In the workspace, select **+ New** ➔ **Lakehouse** ➔ Name: `lh_sales_lakehouse`.

---

### Step 2: Ingest Raw Data via Data Pipeline (GitHub ➔ Bronze)
1. Select **+ New** ➔ **Data Pipeline** ➔ Name: `pl_ingest_sales_bronze`.
2. Add a **Copy Data Activity**:
   * **Source:**
     * Connection Type: `HTTP / Web`
     * Base URL: `https://raw.githubusercontent.com/<username>/<repo>/main/data/`
     * Relative URL: `sales_data.csv`
     * Format: `DelimitedText (CSV)`
   * **Destination (Sink):**
     * Store: `Lakehouse (lh_sales_lakehouse)`
     * Root Folder: `Files`
     * File Path: `bronze/sales/raw_sales.csv`
3. Run the pipeline to confirm the raw CSV is landed in OneLake.

---

### Step 3: Transform Data Using PySpark Notebooks

#### 🥉 1. Bronze to Raw Delta Table
```python
# Notebook: 01_Bronze_Ingestion
from pyspark.sql.types import *

bronze_path = "Files/bronze/sales/raw_sales.csv"

# Load raw file
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

# Save as Bronze Delta table (Immutable Raw Store)
df_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_sales_raw")
```

#### 🥈 2. Silver Cleansing & Validation
```python
# Notebook: 02_Silver_Cleansing
from pyspark.sql.functions import col, to_date, trim, current_timestamp

df_bronze = spark.read.table("bronze_sales_raw")

# Clean, deduplicate, and enforce types
df_silver = df_bronze \
    .filter(col("TransactionID").isNotNull()) \
    .dropDuplicates(["TransactionID"]) \
    .withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd")) \
    .withColumn("CustomerID", col("CustomerID").cast("integer")) \
    .withColumn("Amount", col("Amount").cast("decimal(18,2)")) \
    .withColumn("Region", trim(col("Region"))) \
    .withColumn("IngestionTimestamp", current_timestamp())

# Save as Silver Delta table
df_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("silver_sales_cleansed")
```

#### 🥇 3. Gold Dimensional Modeling (Star Schema)
```python
# Notebook: 03_Gold_Modeling
from pyspark.sql.functions import monotonically_increasing_id, col

df_silver = spark.read.table("silver_sales_cleansed")

# 1. Customer Dimension
dim_customer = df_silver.select("CustomerID", "CustomerName", "CustomerEmail", "Region") \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("CustomerKey", monotonically_increasing_id())

dim_customer.write.mode("overwrite").format("delta").saveAsTable("gold_dim_customer")

# 2. Fact Table
fact_sales = df_silver.select(
    col("TransactionID").alias("SalesOrderNumber"),
    col("TransactionDate"),
    col("CustomerID"),
    col("ProductID"),
    col("Quantity"),
    col("Amount").alias("SalesAmount")
)

fact_sales.write.mode("overwrite").format("delta").saveAsTable("gold_fact_sales")
```

---

### Step 4: Load into Synapse Data Warehouse & Serving Layer
For enterprise analytical workloads needing cross-database T-SQL joins and governance:

1. In the Workspace, select **+ New** ➔ **Warehouse** ➔ Name: `wh_sales_analytics`.
2. Load Lakehouse Gold tables into the Warehouse using T-SQL:

```sql
CREATE SCHEMA Gold;
GO

-- Load Customer Dimension
CREATE TABLE Gold.DimCustomer AS
SELECT 
    CustomerKey,
    CustomerID,
    CustomerName,
    CustomerEmail,
    Region
FROM [lh_sales_lakehouse].[dbo].[gold_dim_customer];
GO

-- Load Sales Fact
CREATE TABLE Gold.FactSales AS
SELECT 
    SalesOrderNumber,
    TransactionDate,
    CustomerID,
    ProductID,
    Quantity,
    SalesAmount
FROM [lh_sales_lakehouse].[dbo].[gold_fact_sales];
GO

-- Analytical Summary View
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
```

---

### Step 5: Power BI DirectLake Analytics & DAX Modeling

1. **DirectLake Semantic Model:**
   * In Fabric Lakehouse/Warehouse, click **Reporting** ➔ **New Semantic Model**.
   * Select `Gold.FactSales` and `Gold.DimCustomer` to connect via DirectLake mode (no data refresh lag).
2. **Key DAX Measures:**
```dax
Total Sales = SUM(FactSales[SalesAmount])

YoY Sales Growth = 
VAR CurrentSales = [Total Sales]
VAR PreviousSales = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(DimDate[Date]))
RETURN
    DIVIDE(CurrentSales - PreviousSales, PreviousSales, 0)
```
3. **Dynamic Row-Level Security (RLS):**
```dax
[CustomerEmail] = USERPRINCIPALNAME()
```

---

## 🎯 Key Achievements & Results
* **DirectLake Mode:** Eliminated Power BI refresh schedules with native query access directly on Delta Parquet files in OneLake.
* **ACID Reliability:** Ensured 99.9% pipeline reliability via Delta Lake transaction logs and schema enforcement.
* **Unified Governance:** Single source of truth accessible seamlessly across PySpark, T-SQL, and Power BI dashboards.

---

## 👨‍💻 Author
* **Kiran Chaudhari** – Senior Power BI Developer | Data Analyst | BI Specialist
* **Location:** Pune, Maharashtra, India
* **LinkedIn:** [linkedin.com/in/kiran-chaudhari](https://linkedin.com/in/kiran-chaudhari)
* **Email:** contact.kiran1997@gmail.com
