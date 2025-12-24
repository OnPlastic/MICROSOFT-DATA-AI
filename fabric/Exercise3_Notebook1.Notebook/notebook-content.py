# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "86add053-f7b2-44c0-a5a5-5ca270ddcf6d",
# META       "default_lakehouse_name": "dp_600_lakehouse",
# META       "default_lakehouse_workspace_id": "63318995-edf3-4ecd-a82e-fc8e0560ed46",
# META       "known_lakehouses": [
# META         {
# META           "id": "86add053-f7b2-44c0-a5a5-5ca270ddcf6d"
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

# # Exercise2 - Analyze data with Apche Spark in Fabric
# 
# ---
# - Create a workspace: "**dp-600_workspace**" vorhanden
# - Create a lakehouse: "**dp-600_lakehouse**", vorhanden
# - Upload files: "**product.csv**" into _Files/data_ - Link: [GitHub.Microsoftlearning](https://github.com/MicrosoftLearning/dp-data/raw/main/products.csv)
# 
# - Create a Notebook: "**Exercise3_Notebook1**"

# CELL ********************

# Delta-Lake tables
# Use this Notebook to explore Delta Lake functionallity

# Connect existing data sources
# use the "Datenelement hinzufügen" button and connect the existing "dp_600_lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Explore data in a DataFrame
# ---
# You have a workspace, a lakehouse and a notebook now you are ready to work with the data.

# CELL ********************

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType


# define the schema

schema = StructType() \
.add("ProductID", IntegerType(), True) \
.add("ProductName", StringType(), True) \
.add("Category", StringType(), True) \
.add("ListPrice", DoubleType(), True)


df = spark.read.format("csv").option("header","true").schema(schema).load("Files/data/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/data/products.csv"


# Bedenke wieder die display() Probleme in den vergangen Übungen! Use toPandas
# display(df) - besser (df.limit(10).toPandas)

display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Delta Tables
# 
# - **Managed** Fabric manages both the schema and the data files
# - **External** tables allow you to store data externally, with managed metadata by Fabric
# 
# ### Create a managed table
# 
# ---
# 


# CELL ********************

# To create a managed Delta table use 

df.write.format("delta").saveAsTable("managed_products")

# !!! Achtung !!!

# Um nicht einen Fehler zu erhalten wenn du diese Zelle erneut ausführst,
# musst du die bereits existierende Zelle überschreiben
# .mode("overwrite") --> überschreibt die Tabelle
# .opton("overwriteSchema", "true") --> überschreibt die Metadaten

'''
(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable("managed_products")
)
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Um Bilder im Notebook anzeigen zu können kannst du nicht die Markdown Image-Einfügen Option verwenden.
# Du musst sie von python rendern lassen!!


from IPython.display import Image, display

display(
        Image(filename="/lakehouse/default/Files/data/saved_products_DeltaTable.png")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create an external table
# 
# ---
# 
# The tabels can be stored somewhere ohter than the lakehouse, but with schema metadata stored in the lakehouse.


# CELL ********************

# Die Anweisung mit der Pfadangbe in eine Zeile zu schreiben ist nicht gut lesbar!

# df.write.format("delta").saveAsTable("external_products", path="abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products")


# Deswegen ein kleiner Workaround - Semantische Aufteilung:

external_path = (
    "abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/"
    "86add053-f7b2-44c0-a5a5-5ca270ddcf6d/"
    "Files/external_products"
)


# Nächste Stolperfalle -saveAsTable wenn eine Tabelle EIN mal als managed gespeichert wurde, dann erzeugt
# saveAsTable immmer wieder eine managed table !!!

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable("external_products", path=external_path)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED managed_products;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED external_products;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/external_products/part-00000-3214a473-344d-4816-8f4d-dd40718a47e2-c000.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/external_products/part-00000-3214a473-344d-4816-8f4d-dd40718a47e2-c000.snappy.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_products2", path="abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products2")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").save(path="abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products3")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/external_products3/part-00000-7e4b209a-be3b-424a-92d5-1458bc58289a-c000.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/external_products3/part-00000-7e4b209a-be3b-424a-92d5-1458bc58289a-c000.snappy.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE products
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/external_products3';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
