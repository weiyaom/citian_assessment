# NYC Taxi Data Pipeline (Airflow + dbt + Postgres)

This project demonstrates an end-to-end data engineering workflow:

## 🧱 Components
- Apache Airflow (download + validate + load raw data)
- PostgreSQL (raw + analytics schema)
- dbt (dimensional modeling + transformations)

---

## 1. Airflow Pipeline
### Tasks:
1. Download monthly Parquet file from NYC TLC CDN  
2. Validate and clean data  
3. Load cleaned data into Postgres using COPY (high performance)  
4. Incremental processing using Airflow `@monthly` schedule  

---

## 2. dbt Models

### Dimensional Models
- **dim_locations**  
  Extract unique pickup & dropoff locations and enrich with TLC lookup.

- **dim_time**  
  Time dimension for analytics usage.

- **fact_trips**  
  Main fact table with metrics and foreign keys.

Supports incremental loading.

---

## 3. SQL Tests
Implemented dbt tests:
- uniqueness
- non-null checks
- relationship tests  

---

## 4. How to Run

```bash
dbt deps
dbt seed
dbt run
dbt test
