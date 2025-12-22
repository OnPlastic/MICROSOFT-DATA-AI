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

# Wie weiter unten im Notebook festgestellt, kann display() zickig sein!
# Es hat sich herausgestellt dass die sichere Methode "display(dataframe.limit().toPandas())" ist!!!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exercise2 - Analyze data with Apche Spark in Fabric
# 
# ---
# 1. Create a workspace: "**dp-600_workspace**" vorhanden
# 2. Create a lakehouse: "**dp-600_lakehouse**", vorhanden
# 3. Upload files: Folder "**orders**" with sales files
# 4. Create a Notebook: "**Exercise2_Notebook1**" 


# MARKDOWN ********************

# ## Create a DataFrame
# ---
# You have a workspace, a lakehouse and a notebook now you are ready to work with the data.


# CELL ********************

df = spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")
# "df" now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".

display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Descriptive column names help you make sense of data.

from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

# first step - df = spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")
# final solution
df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Explore data in a DataFrame
# 
# ### Filter a DataFrame
# ---

# CELL ********************

# The folowing Code filters the data so that only two columns are returned.
# It also "count" and "distinct" to summarize the number of records

customers = df['CustomerName', 'Email']


# first step - provides only numbers

# print(customers.count())
# print(customers.distinct().count())


# better solution - described numbers with f-string declaration

total_customers = customers.count()
distinct_customers = customers.distinct().count()

print(f"Kunden insgesamt: {total_customers}")
print(f"Kunden eindeutig: {distinct_customers}")

display(customers.distinct().limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Another way of achieving the same result is to use the "select" method.


# first step - create a new DataFrame called customers

customers = df.select("CustomerName", "Email")


# second step - modified code, using "select" & "wehre", you also can "cain" multiple funktions
# code to select only the customers who have purchased the Road-250 Red, 52 product.

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')


# noch eine Möglichkeit Beschriftungen einzufügen

print("Kunden insgesamt:", customers.count())
print("Kunden eindeutig:", customers.distinct().count())

display(customers.distinct().limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Aggregate and group data in a DataFrame
# ---

# CELL ********************

# Aggregate and group data in a DataFrame

productSales = df.select("Item", "Quantity").groupBy("Item").sum()

display(productSales.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# The results now show the number of sales orders per year

yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

display(yearlySales.limit(10).toPandas())

# The 'import' statement enables to use Spark SQL library.
# The 'select' method is used with a SQL year funktion to extract the year component of the 'OrderDate' field.
# The 'alias' method is used to assign a column name to the extracted year value.
# The 'groupBy' method groups the data by the derived Year column.abs
# The 'count' of rows in each group is calculated before the 'orderBy' method is used to sort the resulting DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Use Spark to transform data files
# 
# ### Use DataFrame methods and funktions to transform data
# 
# ---
# 
# Common tasks for engineers and scientists is to transform data for further downstream processing.


# CELL ********************

from pyspark.sql.functions import *


# Create Year and Month columns
# transformed_df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))
transformed_df = (
    df
    .withColumn("Year", year(col("OrderDate")))
    .withColumn("Month", month(col("OrderDate")))
)

# Create the new FirstName and LastName fields
# transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), "").getItem(0)).withColum("LastName", split(col("CustomerName"), "").getItem(1))
transformed_df = (
    transformed_df
    .withColumn("FirstName", split(col("CustomerName"), " ").getItem(0))
    .withColumn("LastName", split(col("CustomerName"), " ").getItem(1))
)


# Filter and reorder columns
# transformed_df = transformed_df["SaleOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]
transformed_df = transformed_df[
    "SalesOrderNumber",
    "SalesOrderLineNumber",
    "OrderDate",
    "Year",
    "Month",
    "FirstName",
    "LastName",
    "Email",
    "Item",
    "Quantity",
    "UnitPrice",
    "Tax"
]

# Display the first five orders

display(transformed_df.limit(5).toPandas())


# A new DataFrame is generated from the original order data with these 'Transformations'
# 'Year' and 'Month' columns added, based on the OrderDate column.abs
# 'FirstName' and 'LastName' columns added, based on the 'CustomerName' column.
# The columns are filtered and reordered, and the 'CustomerName' column removed.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save transformed data
# 
# ---
# 
# At this point it comes handy to save the transformed data so that it can be used for further analysis.

# CELL ********************

# Save the transformed DataFrame in Parquet format

# create new Folders and save parquet files in it
transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')

print("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from IPython.display import Image, display

display(
    Image(filename="/lakehouse/default/Files/data/save_transformed_data.jpg")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Die Parquetfiles wurden erstellt!</mark>

# CELL ********************

# A new DataFrame is created from the parquet files 'transformed_data/orders' folder

orders_df = spark.read.format("parquet").load("Files/transformed_data/orders")

display(orders_df.limit(10).toPandas())
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(orders_df.limit(10))   --Führt zur SchemaAnzeige siehe unten

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Wie sich herausstellt kann 'display' zickig sein!
# Es wird keine Tabelle grendert! Es sind aber Daten vorhanden ->count()

# Ich verwende die echte Table-Preview

orders_df.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Zur Sicherheit nochmal die .toPandas() Methode

display(orders_df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>.limit().toPandas() - rockt!</mark>

# MARKDOWN ********************

# ### Save data in partitioned files
# 
# ---
# 
# Improve performance by partitioning

# CELL ********************

# Save the DataFrame, partitioning the data by 'Year' and 'Month'

orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")

display(orders_df.limit(10).toPandas())

print ("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from IPython.display import Image, display

display(
    Image(filename="/lakehouse/default/Files/data/save_partitioned_data.jpg")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Jetzt greifen wir auf die partitionierten Daten zu

orders_2021_df = spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=*")

display(orders_2021_df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Notice that the partitioning columns specified in the path (Year and Month) are not included in the DataFrame.</mark>

# MARKDOWN ********************

# ## Work with tabels and SQL
# 
# ### Create a table
# 
# ---
# 
# The Spark SQL library supports the use of SQL statements to query tables in the metastore.

# CELL ********************

# Create a table
df.write.format("delta").saveAsTable("dbo.salesorders")

# Get the table description
spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ```
# [!NOTE] In this example, no explicit path is provided, so the files for the table will be managed by the metastore. Also, the table is saved in delta format which adds relational database capabilities to tables. This includes support for transactions, row versioning, and other useful features. Creating tables in delta format is preferred for data lakehouses in Fabric.
# ```

# CELL ********************

# In the … menu for the salesorders table, select Load data > Spark.

# This cell will be createt with that code
df = spark.sql("SELECT * FROM dp_600_lakehouse.dbo.salesorders LIMIT 1000")

display(df.limit(10).toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run SQL code in a cell
# 
# ---
# 
# Embed SQL statements into a cell containing PySpark code.

# CELL ********************

# MAGIC %%sql
# MAGIC /*
# MAGIC wie in manchen Markdown Codeblöcken, zuerst die Sprache angeben
# MAGIC dies muss aber ganz am Anfang stehen und die Kommentare sehen anders aus.
# MAGIC */
# MAGIC 
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC     SUM((UnitPrice * Quantity) + Tax) as GrossRevenue
# MAGIC FROM salesorders
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC ORDER BY OrderYear;
# MAGIC 
# MAGIC -- %%sql this command is called 'a magic'
# MAGIC -- SQL code references 'salesorders' table created above
# MAGIC -- output from the query is directly displayed under the cell

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Visualize data with Spark
# 
# ### View results as a chart
# 
# ---
# 
# Fabric notebooks include a built-in chart view but it is not for complex charts.

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM salesorders
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Run the code to display data from 'salesorders' in the results section beneath the cell, select **<mark>+New chart</mark>**.
# 
# Use the <mark>**Build my own**</mark> button at the bottom-right of the results section and set the chart settings.
# 
# - Chart type: Bar chart
# - X-axis: Item
# - Y-axis: Quantity
# - Series Group: leave blank
# - Aggregation: Sum
# - Missing and NULL values: Display as 0
# - Stacked: Unselected

# MARKDOWN ********************

# ### Get startet with matplotlib
# 
# ---

# CELL ********************

# SQL-Abfrage als String definieren
sqlQuery = """
SELECT
    --Jahr aus dem OrderDate extrahieren
    -- YEAR(OrderDate) liefert eine Zahl (z.B. 2021)
    -- CAST(...AS CHAR(4)) wandelt sie in einen String um
    CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear,

    -- Umsatz pro Bestellung:
    -- UnitPrice * Quantity = Nettowert
    -- + Tax = Bruttowert
    SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue,

    -- Anzahl eindeutiger Bestellungen pro Jahr
    -- DISTINCT, weil eine Bestellung mehrere Zeilen haben kann
    COUNT(DISTINCT SalesOrderNumber) AS YearlyCounts

FROM dbo.salesorders

-- Gruppierung nach Jahr (muss exakt dem SELECT-Ausdruck entsprechen)
GROUP BY CAST(YEAR(OrderDate) AS CHAR(4))

-- Sortierung nach Jahr (alphabetisch korrekt, da 4-stelliges Jahr)
ORDER BY OrderYear
"""

# Ausführen der SQL-Abfrage im Spark SQL Engine
# Ergebnis ist wieder ein Spark DataFrame
df_spark = spark.sql(sqlQuery)

# Ausgabe der Ergebnisse in der Konsole (Text-Tabellenform)
df_spark.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt

# matplotlib requires a Pandas dataframe, not a Spark one!!!
df_sales = df_spark.toPandas()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

# Display the plot
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Modify the code to plot the chart as follows</mark>

# CELL ********************

from matplotlib import pyplot as pyplot

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(8,3))

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

# Show the figure
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>A figure can contain multiple subplots, each on its own axis. Modify the code to plot the chart as follows</mark>

# CELL ********************

from matplotlib import pyplot as plt

# Clear the plot area
plt.clf()

# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))

# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')

# Create a pie chart of yearly order counts on the second axis
ax[1].pie(df_sales['YearlyCounts'])
ax[1].set_title('Orders per Year')
ax[1].legend(df_sales['OrderYear'])

# Add a title to the Figure
fig.suptitle('Sales Data')

# Show the figure
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ```
# 
#     [!NOTE] To learn more about plotting with matplotlib, see the matplotlib documentation.
# 
# ```

# MARKDOWN ********************

# ### Use the seaborn library
# 
# ---
# 
# Some chart types can require more complex code to achieve best results. For this reason, new libraries have been built on matplotlib.

# CELL ********************

import seaborn as sns

# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Modify the code as follows</mark>

# CELL ********************

import seaborn as sns

# Clear the plot area
plt.clf()

# Set the visual theme for seaborn
sns.set_theme(style="whitegrid")

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import seaborn as sns

# Clear the plot area
plt.clf()

# Create a line chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Umgang mit `inf` und `NaN` bei Visualisierungen (Seaborn / Pandas)
# 
# ### 1. Was ist `inf`?
# `inf` steht für *Infinity* (Unendlichkeit).  
# Es entsteht durch mathematische Operationen wie Division durch 0 oder Logarithmen von 0.
# 
# Beispiele:
# - `1 / 0  → inf`
# - `np.log(0) → -inf`
# 
# `inf` ist technisch ein numerischer Wert, aber kein sinnvoll darstellbarer Zahlenwert.
# 
# ---
# 
# ### 2. Was ist `NaN`?
# `NaN` bedeutet *Not a Number* und steht für fehlende oder ungültige Werte.
# 
# Typische Ursachen:
# - fehlende Daten
# - ungültige Berechnungen
# - bewusst als „leer“ markierte Werte
# 
# `NaN` signalisiert: *Hier existiert kein sinnvoller Wert.*
# 
# ---
# 
# ### 3. Unterschied zwischen `inf` und `NaN`
# 
# | Wert  | Bedeutung            | Interpretation                |
# |------|----------------------|--------------------------------|
# | inf  | Unendlich groß       | Ergebnis ist mathematisch explodiert |
# | -inf | Unendlich klein      | Ergebnis ist mathematisch kollabiert |
# | NaN  | Kein Zahlenwert      | Wert fehlt oder ist ungültig   |
# 
# ---
# 
# ### 4. Was bedeutet „`inf` wurde wie `NaN` behandelt“?
# Früher konnte Pandas so konfiguriert werden, dass `inf` und `-inf` intern wie `NaN` behandelt wurden.
# 
# Das bedeutete:
# - `inf` wurde ignoriert
# - `inf` wurde nicht geplottet
# - `inf` wurde bei Aggregationen übersprungen
# 
# ---
# 
# ### 5. Warum wird `use_inf_as_na` abgeschafft?
# Die Option war:
# - global
# - implizit
# - schwer nachvollziehbar
# 
# Moderne Datenanalyse folgt dem Prinzip:
# > **Explizit ist besser als implizit**
# 
# Deshalb soll die Umwandlung künftig bewusst im Code erfolgen.
# 
# ---
# 
# ### 6. Empfohlene Vorgehensweise
# Unendliche Werte werden **explizit** in `NaN` umgewandelt:
# 
# ```python
# df = df.replace([np.inf, -np.inf], np.nan)


# CELL ********************

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# --- Check for infinite values ---------------------------------

has_inf = df_sales.isin([np.inf, -np.inf]).any().any()

if has_inf:
    print("⚠️ Unendliche Werte (inf / -inf) gefunden – werden in NaN umgewandelt.")
    df_sales = df_sales.replace([np.inf, -np.inf], np.nan)
else:
    print("✅ Keine unendlichen Werte gefunden.")

# --- Clear the plot area ----------------------------------------

plt.clf()

# --- Create a line chart ---------------------------------------

ax = sns.lineplot(
    x="OrderYear",
    y="GrossRevenue",
    data=df_sales
)

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Hinweis zur Warnmeldung (`FutureWarning`)**
# 
# Vor dem Plotten wird explizit geprüft, ob unendliche Werte (`inf`, `-inf`) in den Daten vorkommen und diese ggf. in `NaN` umgewandelt.  
# Damit ist der Code fachlich korrekt und zukunftssicher im Sinne der Pandas-Empfehlung.
# 
# Die weiterhin ausgegebene Warnmeldung entsteht unabhängig von den konkreten Daten, da Seaborn intern noch eine veraltete Pandas-Option verwendet.  
# Der Plot ist korrekt, und der Code ist bereits so vorbereitet, dass er auch bei zukünftigen Änderungen weiterhin funktioniert.</mark>


# MARKDOWN ********************

# **Die Übung ist beendet**
