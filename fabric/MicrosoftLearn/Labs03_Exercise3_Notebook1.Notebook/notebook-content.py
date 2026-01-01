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

# CELL ********************

# Um Bilder im Notebook anzeigen zu können kannst du nicht die Markdown Image-Einfügen Option verwenden.
# Du musst sie von python rendern lassen!!


from IPython.display import Image, display

display(
        Image(filename="/lakehouse/default/Files/screenshots/labs3_saved_products_managed_delta_table.png")
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
# **1**. In the Explorer pane, in the … menu for the Files folder, select Copy ABFS path. The ABFS path is the fully qualified path to the lakehouse Files folder.
# 
# **2**. In a new code cell, paste the ABFS path. Add the following code, using cut and paste to insert the abfs_path into the correct place in the code:


# CELL ********************

# Die Anweisung mit der Pfadangbe in eine Zeile zu schreiben ist nicht gut lesbar!

# df.write.format("delta").saveAsTable("external_products", path="abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/d5910357-b74a-4308-8558-85c44973718b/Files")



# Deswegen ein kleiner Workaround - Semantische Aufteilung:

external_path = (
    "abfss://63318995-edf3-4ecd-a82e-fc8e0560ed46@onelake.dfs.fabric.microsoft.com/"
    "d5910357-b74a-4308-8558-85c44973718b/"
    "Files/external_products"
)

# Nächste Stolperfalle -saveAsTable wenn eine Tabelle EIN mal als managed gespeichert wurde, dann erzeugt
# saveAsTable immmer wieder eine managed table. -> Deswegen zuerst nur "save" und dann "explizit" registrieren.

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable(
        "external_products",
        path=external_path
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Hinweise**
# - Hier soll ein neuer Ordner "**external_products**" erzeugt werden
# - Der <mark>ABFS Pfad</mark> bezieht sich auf den "<mark>Files</mark>" folder.
# - Im Code muss deswegen beides zusammengesetzt werden. <mark>ABFS-Pfad</mark> + **/external_prducts**


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

# MARKDOWN ********************

# Das hat funktioniert! Die Tabelle ist - Type: <mark>MANAGED</mark>; Location: ...Tables/<mark>dbo/managed_products</mark>

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**ICH HALTE FEST AN DIESER STELLE, DIE ERSTELLUNG VON "EXTERNEN"-TABELLEN FUNKTIONIERT NICHT !!!**</mark> 
# 
# Das habe ich mit versch. Einstellungen probiert. save(...); savaAsTable(...); er legt zwar den Ordner an,  
# aber die Tabellen Daten landen immer in einer "managed"-Table

# CELL ********************

from IPython.display import Image, display

display(
        Image(filename="/lakehouse/default/Files/screenshots/labs3_saved_products_external_managed_delta_table-fail.png")
)

display(
        Image(filename="/lakehouse/default/Files/screenshots/labs3_saved_products_external_managed_delta_table-fail2.png")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Bereinigen der Tabellen für den nächsten Schritt:
# MAGIC 
# MAGIC DROP TABLE managed_products;
# MAGIC DROP TABLE external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Use SQL to create a Delta table
# 
# Now lets create a Delta table, using the "magic"-command **%%sql**


# MARKDOWN ********************

# 1. 

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE products
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/external_products';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
