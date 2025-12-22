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

-- Exercise 1: Umsatzanalyse nach Artikel
-- Quelle: Lakehouse-Tabelle "sales"
-- Ziel: Berechnung des Gesamtumsatzes pro Artikel
-- Umsatz = Menge (Quantity) * Einzelpreis (UnitPrice)

SELECT
    Item,                                   -- Artikelname
    SUM(Quantity * UnitPrice) AS Revenue   -- Gesamtumsatz pro Artikel
FROM sales
GROUP BY Item                              -- Aggregation pro Artikel
ORDER BY Revenue DESC;                     -- Sortierung: höchster Umsatz zuerst


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- **Übung beendet**
