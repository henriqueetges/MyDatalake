**MyDatalake**

This project is a sandbox for me to capture different kinds of data and load them into databricks environment.
Goal is to implement a medallion architecture. This is being built on top of a free version of Databricks, 
so some features may not be explored at this time.

**Data Architecture**
- Raw Layer: File data stored in managed storage, using json files
- Bronze Layer: Delta lake tables, based on raw data
- Silver Layer: Denormalized and cleansed tables
- Gold Layer: Reporting Layer

**Project Structure**
- Lib: Contains helper functions
- Layers: Contain the ingestion scripts, their scripts, queries and metadata stored as yml files.


