-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "86add053-f7b2-44c0-a5a5-5ca270ddcf6d",
-- META       "default_lakehouse_name": "dp_600_lakehouse",
-- META       "default_lakehouse_workspace_id": "63318995-edf3-4ecd-a82e-fc8e0560ed46",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "86add053-f7b2-44c0-a5a5-5ca270ddcf6d"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- Exercise 1: Umsatzanalyse nach Artikel
-- MAGIC -- Quelle: Lakehouse-Tabelle "sales"
-- MAGIC -- Ziel: Berechnung des Gesamtumsatzes pro Artikel
-- MAGIC -- Umsatz = Menge (Quantity) * Einzelpreis (UnitPrice)
-- MAGIC 
-- MAGIC SELECT
-- MAGIC     Item,                                   -- Artikelname
-- MAGIC     SUM(Quantity * UnitPrice) AS Revenue   -- Gesamtumsatz pro Artikel
-- MAGIC FROM sales
-- MAGIC GROUP BY Item                              -- Aggregation pro Artikel
-- MAGIC ORDER BY Revenue DESC                     -- Sortierung: höchster Umsatz zuerst
-- MAGIC LIMIT 10;
-- MAGIC 
-- MAGIC -- Kann nicht im PySpark-Kernel über VS-Code ausgeführt werden ohne Angabe von %%sql !!!


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- **Übung beendet**
