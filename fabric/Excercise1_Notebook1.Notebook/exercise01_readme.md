# Exercise 01 – Sales Umsatzanalyse (Microsoft Fabric)

## 🎯 Ziel der Übung
Ziel dieser Übung ist es, ein erstes **Lakehouse in Microsoft Fabric** zu erstellen,
Daten in dieses Lakehouse zu laden und mithilfe von **SQL in einem Notebook**
eine einfache Umsatzanalyse durchzuführen.

Der Fokus liegt auf:
- Arbeiten mit Lakehouse-Tabellen in Microsoft Fabric
- Nutzung von SQL innerhalb eines Fabric Notebooks
- Aggregationen und Sortierungen von Daten
- reproduzierbarer Analyse mittels Notebooks
- Versionierung über GitHub

---

## 📦 Datengrundlage
- **Quelle:** `sales.csv`
- **Speicherort:** Microsoft Fabric Lakehouse
- **Tabelle:** `sales`
- **Spalten:**
  - `Item`
  - `Quantity`
  - `UnitPrice`

Die CSV-Datei wird in das Lakehouse geladen und dort als Tabelle zur Verfügung gestellt.

---

## 🛠️ Verwendete Technologien
- Microsoft Fabric
- Fabric Notebook
- Lakehouse
- SQL (Spark SQL)

---

## 🧮 Analyseschritte

### 1️⃣ Erstellen eines Lakehouse
Im Workspace wird ein neues Lakehouse angelegt (`dp_600_lakehouse`).
Dieses dient als zentraler Speicherort für die Übungsdaten.

---

### 2️⃣ Laden der CSV-Datei in das Lakehouse
Die Datei `sales.csv` wird in den **Files-Bereich** des Lakehouse hochgeladen
und anschließend als Tabelle registriert.

---

### 3️⃣ Erste Abfrage der Daten
Über eine SQL-Zelle im Notebook werden die geladenen Daten geprüft.

```sql
SELECT *
FROM sales;
```

---

### 4️⃣ Berechnung des Umsatzes pro Produkt
Der Umsatz wird als Produkt aus `Quantity` und `UnitPrice` berechnet
und anschließend pro Artikel aggregiert.

```sql
SELECT
    Item,
    SUM(Quantity * UnitPrice) AS Revenue
FROM sales
GROUP BY Item
ORDER BY Revenue DESC;
```

---

## 📌 Erkenntnisse
- Daten können direkt im Lakehouse gespeichert und per SQL analysiert werden
- SQL eignet sich sehr gut für einfache analytische Fragestellungen
- Fabric Notebooks ermöglichen reproduzierbare Analysen
- Die Ergebnisse bilden die Grundlage für weiterführende Analysen und Visualisierungen

---

## 🔗 Einordnung im Lernpfad
Diese Übung stellt den **Einstieg in Microsoft Fabric** dar und bildet
die Grundlage für weiterführende Übungen mit PySpark, Visualisierung
und Analytics.

Sie ist Bestandteil der Vorbereitung auf die
**DP-600 – Microsoft Fabric Analytics Engineer** Zertifizierung.
