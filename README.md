# sql-data-warehouse-project
Building a modern data warehouse with SQL Server, including ETL processes, data modeling and analytics
---
 Data Architecture
 
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
<img width="1110" height="674" alt="Data Architecture" src="https://github.com/user-attachments/assets/168267e1-854e-4368-b6b1-2a468c9e9fd8" />

1.Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.

2.Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

3.Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

### Project Requirements

### Building the Data Warehouse

#### Objective
Develop a modern data warehouse using sql Server to Consolidate sales data, enabling analytical reporting and informed decision making.

#### Specifications
- **Data Source**: Import data from two sources systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integeration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scopes**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both buisness stakeholders and analytics teams.

---
### BI: Analytics & Reporting (Data Analytics)

#### Objective
Develop SQl-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performence**
- **Sales Trends**
  These insights empower stakeholders with key buisness metrics, enabling strategic decision-making.
