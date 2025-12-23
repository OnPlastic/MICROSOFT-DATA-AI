# Exercise 02 – Umsatzanalyse mit PySpark & Visualisierung (Microsoft Fabric)

## 🎯 Ziel der Übung
Ziel dieser Übung ist es, die in Exercise 01 erstellten Lakehouse-Daten mit **PySpark**
weiterzuverarbeiten und die Ergebnisse mithilfe von **Python-Visualisierungen**
anschaulich darzustellen.

Der Fokus liegt auf:
- Arbeiten mit PySpark DataFrames in Microsoft Fabric
- Aggregationen und Transformationen auf Spark-Ebene
- Übergang von verteiltem Compute (Spark) zu lokaler Visualisierung
- Erstellung einfacher Diagramme mit seaborn
- Verständnis des Zusammenspiels von Spark, pandas und Visualisierung

---

## 📦 Datengrundlage
- **Quelle:** `sales.csv`
- **Speicherort:** Microsoft Fabric Lakehouse
- **Tabelle:** `sales`
- **Schlüsselspalten:**
  - `Item`
  - `Quantity`
  - `UnitPrice`

Die Daten wurden bereits in Exercise 01 in das Lakehouse geladen.

---

## 🛠️ Verwendete Technologien
- Microsoft Fabric Notebook
- Apache Spark (PySpark)
- pandas
- seaborn
- matplotlib

---

## 🧮 Analyseschritte

### 1️⃣ Laden der Lakehouse-Tabelle als Spark DataFrame
```python
df = spark.read.table("sales")
```

---

### 2️⃣ Berechnung des Umsatzes pro Produkt
```python
from pyspark.sql.functions import col, sum as _sum

revenue_df = (
    df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
      .groupBy("Item")
      .agg(_sum("Revenue").alias("TotalRevenue"))
      .orderBy(col("TotalRevenue").desc())
)
```

---

### 3️⃣ Konvertierung in pandas DataFrame
```python
pdf = revenue_df.toPandas()
```

⚠️ Hinweis:  
Dieser Schritt ist nur für **kleine bis mittlere Ergebnisdatenmengen** geeignet.
Die eigentliche Berechnung erfolgt weiterhin vollständig auf Spark-Ebene.

---

## 📊 Visualisierung der Ergebnisse

### 4️⃣ Balkendiagramm: Umsatz pro Produkt
```python
import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
sns.barplot(
    data=pdf,
    x="TotalRevenue",
    y="Item",
    palette="viridis"
)

plt.title("Umsatz pro Produkt")
plt.xlabel("Gesamtumsatz")
plt.ylabel("Produkt")
plt.tight_layout()
plt.show()
```

---

## 📌 Erkenntnisse
- Spark eignet sich hervorragend für skalierbare Datenverarbeitung
- Für Visualisierungen ist häufig eine Konvertierung nach pandas sinnvoll
- Microsoft Fabric ermöglicht einen nahtlosen Übergang zwischen
  verteiltem Compute und interaktiver Analyse
- Die Kombination aus Spark + Python + seaborn ist ein typischer
  Data-Engineering- und Analytics-Workflow

---

## 🔗 Einordnung im Lernpfad
Diese Übung baut direkt auf **Exercise 01** auf und erweitert sie um:
- praktische PySpark-Nutzung
- Verständnis für Datenflüsse innerhalb von Fabric
- erste Visualisierungsschritte für Analyse & Reporting
- Vorbereitung auf Power BI und weiterführende Analytics-Szenarien

Sie ist ein zentraler Baustein für die **DP-600 – Microsoft Fabric Analytics Engineer** Zertifizierung.
