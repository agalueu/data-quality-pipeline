# 💼 Business Case: Data Quality Pipeline

## Context

The organization processes a high volume of daily transactions from multiple sources and external systems. Historically, data ingestion has presented several critical challenges:

* **Technical inconsistencies**: Null values, duplicates, and heterogeneous date formats breaking ETL processes
* **Financial risk**: Invalid transaction amounts or inconsistent statuses distorting revenue reporting
* **Lack of trust**: Stakeholders lose confidence in dashboards when data is not properly validated

---

## 🎯 Objective

Transform raw and unreliable data into trusted information assets by implementing an automated **Data Quality Firewall**.

This pipeline serves as the first validation layer in the data architecture, ensuring data integrity before it reaches reporting systems.

---

## 🛠️ Technical Solution

The solution is designed as a modular and extensible pipeline with the following components:

### 1. Data Schema Validation

A strict validation of required columns is performed at the beginning of the process.
If the dataset does not meet the expected structure, the pipeline stops immediately using exceptions (`ValueError`), preventing error propagation.

---

### 2. Data Normalization (Sanitization)

* **Standardization**: Unified date formats and cleaned text fields
* **Robust typing**: Safe type conversion using coercion to identify corrupted records

---

### 3. Business Rules Engine

Data quality rules are applied through explicit validation flags:

* **Amount validation**: Detects invalid values (negative or zero in approved transactions)
* **Field completeness**: Identifies missing mandatory fields (`transaction_id`, `user_id`)
* **Uniqueness control**: Detects duplicate records to prevent double counting

---

### 4. Anomaly Detection

Instead of static thresholds, the pipeline uses the **Interquartile Range (IQR)** method to dynamically detect unusually high transactions.

This approach allows identifying potential system errors or suspicious activity without manually adjusting thresholds.

---

## 📈 Outputs

The pipeline generates three key outputs:

* **Reporting Dataset** → Clean, validated data ready for BI tools (Power BI, Tableau)
* **Anomalies Dataset** → Records requiring manual review
* **Data Quality Summary** → Metrics on data health (errors, duplicates, outliers)

---

## 💡 Impact

* Significant reduction in manual data cleaning effort
* Early detection of data quality issues
* Improved reliability of reporting and analytics
* Increased stakeholder confidence in data-driven decisions

---

## 🧠 Key Takeaway

This pipeline goes beyond data processing — it introduces a **data quality control layer** that improves reliability, scalability, and trust in analytical systems.

