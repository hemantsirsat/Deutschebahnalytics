# 🚆 DeutscheBahnalytics

**DeutscheBahnalytics** is a data analytics project built to monitor and visualize train delays across major German railway stations.  

---

## 🔍 Overview

DeutscheBahnalytics turns raw Deutsche Bahn timetable data into meaningful operational insights.  
It combines **automated ingestion**, **dbt transformations**, and an **interactive Streamlit dashboard** to uncover trends in train delays by hour, location, and type.

The project showcases how a production-ready **data pipeline** can be built and visualized using open-source tools.

---

## 🧩 Business Problem

Deutsche Bahn operates thousands of train connections daily, and delays cause cascading effects across the network, impacting customer satisfaction, operations, and logistics.

Key challenges:
- How do delays vary by **hour of day** and **station**?
- Which stations experience the **most severe** or **frequent** delays?
- Can we identify operational patterns that contribute to recurring bottlenecks?

---

## 💡 Solution Overview

DeutscheBahnalytics provides an **end-to-end data pipeline** and dashboard that:
- Ingests raw **train timetable and delay data** from Deutsche Bahn APIs.
- Transforms and models it using **dbt** (staging → intermediate → mart layers).
- Serves summarized metrics in **Streamlit**, with visual insights like:
  - Average arrival and departure delays by hour.
  - Delay counts per hour.
  - Station-level comparisons across Germany.

---

## ⚙️ Tech Stack

| Component | Description |
|------------|--------------|
| **dbt (Data Build Tool)** | SQL-based transformation and modeling framework. |
| **PostgreSQL (Neon)** | Cloud-hosted database for storing raw and modeled data. |
| **Streamlit** | Web-based analytics app for visualization and exploration. |
| **GitHub Actions** | CI/CD pipelines to orchestrate ingestion and dbt runs. |

---

