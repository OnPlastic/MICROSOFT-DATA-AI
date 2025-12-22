# Microsoft Fabric – DP-600 Lernpfad (Analytics Engineer)

Dieses Verzeichnis enthält alle **Microsoft-Fabric-Items** (Notebooks, Lakehouse),
die im Rahmen meines Lernpfads zur Zertifizierung  
**DP-600 – Microsoft Fabric Analytics Engineer** entstanden sind.

Der Fokus liegt auf:
- strukturiertem Arbeiten mit Microsoft Fabric
- reproduzierbaren Analysen mittels Notebooks
- sauberer Versionierung über GitHub
- praxisnahen Übungen entlang der DP-600-Inhalte

---

## 📂 Struktur dieses Verzeichnisses

```text
fabric/
├── Exercise1_Notebook1.Notebook
├── Exercise2_Notebook1.Notebook
├── dp_600_lakehouse.Lakehouse
├── README.md
```

**Hinweis:**  
Die Ordner mit den Endungen `.Notebook` und `.Lakehouse` werden **automatisch von Microsoft Fabric**
erzeugt und spiegeln den internen Zustand der Fabric-Items wider.  
Diese Struktur sollte **nicht manuell verändert** werden.

---

## 🧪 Übungen

### ▶️ Exercise 01 – Sales Umsatzanalyse (SQL & Lakehouse)
**Inhalt:**
- Anlegen eines Lakehouse in Microsoft Fabric
- Laden einer CSV-Datei (`sales.csv`)
- Erste analytische Abfragen mit SQL
- Aggregation und Sortierung von Umsätzen

**Schwerpunkte:**
- Lakehouse-Grundlagen
- Arbeiten mit Tabellen
- SQL in Fabric Notebooks

📄 Dokumentation:  
→ siehe `Exercise1_Notebook1.Notebook/exercise01_readme.md`

---

### ▶️ Exercise 02 – Umsatzanalyse mit PySpark & Visualisierung
**Inhalt:**
- Weiterverarbeitung der Lakehouse-Daten mit PySpark
- Aggregationen auf Spark-Ebene
- Konvertierung nach pandas
- Visualisierung der Ergebnisse mit seaborn

**Schwerpunkte:**
- PySpark DataFrames
- Übergang von verteiltem Compute zu Visualisierung
- typischer Analytics-Workflow in Fabric

📄 Dokumentation:  
→ siehe `Exercise2_Notebook1.Notebook/exercise02_readme.md`

---

## 🛠️ Verwendete Technologien
- Microsoft Fabric
- Fabric Notebooks
- Lakehouse
- SQL (Spark SQL)
- PySpark
- pandas
- seaborn / matplotlib
- GitHub (Source Control)

---

## 🎓 Einordnung im Lernpfad
Die hier enthaltenen Übungen sind Teil eines strukturierten Lernpfads
zur Vorbereitung auf die **DP-600 – Microsoft Fabric Analytics Engineer**
Zertifizierung.

Sie bilden die Grundlage für weiterführende Themen wie:
- Datenpipelines
- semantische Modelle
- Integration mit Power BI
- Performance-Optimierung in Fabric

---

## ⚠️ Hinweise zur Arbeit mit Fabric & Git
- Notebooks und Lakehouse-Items werden **immer in Fabric** erstellt, umbenannt oder gelöscht
- Git dient ausschließlich der **Versionierung**
- Ordner- und Dateinamen in diesem Verzeichnis sollten **nicht manuell geändert** werden
- Dieses Repo `MICROSOFT-DATA-AI` enthält .md Dateien welche über ein lokales Repo in VS-Code
  gepflegt werden. 
