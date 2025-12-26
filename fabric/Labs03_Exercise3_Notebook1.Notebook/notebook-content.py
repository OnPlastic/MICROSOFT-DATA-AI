# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d5910357-b74a-4308-8558-85c44973718b",
# META       "default_lakehouse_name": "Labs03_Lakehouse",
# META       "default_lakehouse_workspace_id": "63318995-edf3-4ecd-a82e-fc8e0560ed46",
# META       "known_lakehouses": [
# META         {
# META           "id": "d5910357-b74a-4308-8558-85c44973718b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exercise3 - Use Delta Tables in Apache Spark
# 
# ---
# - Create a workspace: "**dp-600_workspace**" vorhanden
# - Create a lakehouse: "**Labs03_Lakehouse**"
# - Upload files: Folder "**products**" with **product.csv** files Link: [GitHub.Microsoftlearning](https://github.com/MicrosoftLearning/dp-data/raw/main/products.csv)
# - Create a Notebook: "**Labs03_Exercise3_Notebook1**"

# MARKDOWN ********************

# ## Create a DataFrame
# ---
# You have a workspace, a lakehouse and a notebook now you are ready to work with the data.

# CELL ********************

# Delta Lake tables
# Use this Notebook to explore Delta Lake functionallity

# Connect existing data sources
# use the "Datenelement hinzufügen" button and connect the existing "dp_600_lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType


# define the schema
schema = StructType() \
.add("ProductID", IntegerType(), True) \
.add("ProductName", StringType(), True) \
.add("Category", StringType(), True) \
.add("ListPrice", DoubleType(), True)

df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products/products.csv"

# Bedenke wieder die display() Probleme in den vergangen Übungen! Use toPandas
# display(df) - besser
display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Delta Tables
# 
# - **<mark>Managed</mark>** - Fabric manages both the schema and the data files
# - **<mark>External</mark>** - tables allow you to store data externally, with managed metadata by Fabric
# 
# ### Create a managed table
# 
# ---
# 


# CELL ********************

# To create a managed Delta table use df.write.format("delta").saveAsTable("managed_products")

# Um nicht einen Fehler zu erhalten wenn du diese Zelle erneut ausführst,
# musst du die bereits existierende Zelle überschreiben
# .mode("overwrite") -- >überschreibt die Tabelle
# .opton("overwriteSchema", "true") --> überschreibt die Metadaten

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable("managed_products")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
