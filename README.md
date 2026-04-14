# Data Quality Pipeline

Automated data quality validation pipeline for transaction datasets.
This project ensures that only clean, validated, and reliable data is used for reporting and analytics.

---

## 🚀 Features

* Schema validation (fail-fast approach)
* Data normalization (dates, amounts, text fields)
* Business rule engine with data quality flags
* Outlier detection using IQR (Interquartile Range)
* Clean reporting dataset generation
* Anomalies dataset for auditing
* Data quality summary metrics

---

## 💼 Business Context

Organizations processing high volumes of transactions often face:

* Inconsistent data formats and missing values
* Duplicate records impacting financial reporting
* Lack of trust in dashboards and analytics

This pipeline acts as a **data quality firewall**, ensuring only validated data reaches downstream systems.

---

## ⚙️ Pipeline Flow

1. Load data
2. Validate schema
3. Normalize fields
4. Apply business rules
5. Detect anomalies (IQR method)
6. Generate outputs

---

## 📈 Outputs

* **Reporting dataset** → Clean and validated data ready for BI tools
* **Anomalies dataset** → Invalid or suspicious records for review
* **Summary report** → Key data quality metrics

---

## 🛠️ Tech Stack

* Python
* Pandas
* YAML (config-driven pipeline)

---

## ▶️ How to Run

```bash
python main.py
```

---

## 💡 Impact

* Reduces manual data cleaning effort (~70–80%)
* Ensures high-quality data for reporting
* Improves trust in business metrics

---

## 📄 Additional Documentation

For a detailed business and technical explanation, see:

👉 `docs/business_case.md`
