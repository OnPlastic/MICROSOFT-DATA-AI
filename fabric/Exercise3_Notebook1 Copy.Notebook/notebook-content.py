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
# - Upload files: Folder "**products**" with **product.csv** files Link: [GitHub.Microsoftlearning](https://github.com/MicrosoftLearning/dp-data/raw/main/products.csv)
# - Create a Notebook: "**Exercise3_Notebook1**"

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
# - **Managed** Fabric manages both the schema and the data files
# - **External** tables allow you to store data externally, with managed metadata by Fabric
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

# 
# ```
#     Erinnerung! das klappt nicht!
# ```
# 
# abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/data/saved_products_DeltaTable.png
# 
# Files/data/saved_products_DeltaTable.png
# 
# /lakehouse/default/Files/data/saved_products_DeltaTable.png
# 
# ![image-alt-text](abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/data/saved_products_DeltaTable.png)
# 
# 
# 


# MARKDOWN ********************

# ### Create an external table
# 
# ---
# 
# The tabels can be stored somewhere ohter than the lakehouse, but with schema metadata stored in the lakehouse.
# 
# 


# CELL ********************

# Die Anweisung mit der Pfadangbe in eine Zeile zu schreiben ist nicht gut lesbar!


df.write.format("delta").saveAsTable("external_products", path="abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products")


# Deswegen ein kleiner Workaround - Semantische Aufteilung:
'''
abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files
external_path = (
    "abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/"
    "86add053-f7b2-44c0-a5a5-5ca270ddcf6d/"
    "Files/external_products"
)
'''
# neuer Versuch
'''
path = 'Files/external_products'
'''
# Nächste Stolperfalle -saveAsTable wenn eine Tabelle EIN mal als managed gespeichert wurde, dann erzeugt
# saveAsTable immmer wieder eine managed table. -> Deswegen zuerst nur "save" und dann "explizit" registrieren.
'''
(df.repartition(2)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .save(path)
)
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %%sql
# CREATE TABLE external_products 
# USING DELTA
# LOCATION 'abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products';

# MARKDOWN ********************

# %%sql
# -- Zuerst löschen falls vorhanden
# 
# DROP TABLE IF EXISTS external_products_ext;

# MARKDOWN ********************

# %%sql
# /* Auch hier kleiner Workaround um die Zeile aufzuteilen. In SQL etwas anders als in Python
#    !!! LOCATION mit ${external_path} hat nicht funktioniert -> path in einer Zeile. */
# 
# CREATE TABLE external_products
# USING DELTA
# LOCATION 'abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/86add053-f7b2-44c0-a5a5-5ca270ddcf6d/Files/external_products';


# MARKDOWN ********************

# **Hinweise**
# - Hier soll ein neuer Ordner "**external_products**" erzeugt werden
# - Der <mark>ABFS Pfad</mark> bezieht sich auf den "<mark>Files</mark>" folder.
# - Im Code muss deswegen beides zusammengesetzt werden. <mark>ABFS-Pfad</mark> + **/external_prducts**


# CELL ********************

from IPython.display import Image, display

display(
        Image(filename="/lakehouse/default/Files/data/saved_products_external_DeltaTable.png")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compare managed and external tables
# 
# ---
# 
# Differences between managed and external tables using the **%sql** <mark>magic command</mark>.

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
