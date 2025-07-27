# End-to-End Facebook Analytics Pipeline

This project solves the challenge of managing 50+ Facebook ad accounts with inconsistent timezones, currencies, and infrastructure. We built a scalable analytics system to centralize data, normalize spend, and support real-time reporting via Power BI—without relying on costly enterprise platforms.

Skills Demonstrated: Data Engineering, Pipeline Monitoring, Architecture Design, Python ETL Scripting

---

## Problem Statement
- Over 50 active FB ad accounts, across `EST`, `UTC`, and `PST` timezones, using `USD`, `EUR`, and `JPY`
- Facebook Ads API lacks native timezone and historical FX normalization
- Stakeholders require a unified dashboard standardized in `USD` and `PST`

---

## Requirements
- Ingest ad spend from all accounts into a centralized BigQuery data warehouse
- Convert timestamps to `PST` and currencies to `USD` using accurate historical FX rates
- Automate hourly ETL batch processing
- Visualize data using Power BI operational dashboards
- Monitor all data transformation scripts via [healthchecks.io](https://healthchecks.io)

---
## Design Approach
<img width="889" height="496" alt="image" src="https://github.com/user-attachments/assets/f74edada-88d9-4883-bd3b-10f9a22bdb9d" />


After evaluating alternatives (`Fivetran`, `Hevodata`), I selected **Coupler.IO** for its simplicity, cost-efficiency, and admin-friendly interface. It allows daily onboarding of new Facebook accounts without engineering overhead.

#### BigQuery Medallion
| Layer  | Purpose                                              |
|--------|------------------------------------------------------|
| Bronze | Raw ad spend + FX rates ingestion                    |
| Silver | Cleaned, unified spend data in `USD` and `PST`       |
| Gold   | Transformations via power query to make use of existing report templates        |

Currency conversion uses a historical FX API to ensure ROI metrics reflect true market conditions.

---

## ETL Workflow

1. **Connect FB Ad Accounts** → via Coupler.IO UI
   <img width="2062" height="1128" alt="image" src="https://github.com/user-attachments/assets/b899aa07-a495-41ce-b7e6-fff891494724" />

3. **Ingest Data** → Raw ad spend + FX rates to BigQuery (Bronze)
   
5. **Transform** → Python script to normalize timezone and FX (Silver)
6. **Visualize** → Power BI loads from Silver and applies final metrics (Gold)
   
8. **Monitor** → Healthchecks.io tracks hourly ETL script uptime, python script also sends error logs through email
   <img width="313" height="203" alt="image" src="https://github.com/user-attachments/assets/6056ec68-57d5-4ac0-ad89-4bb665db01de" />

---

## Notes and Trade-offs

- Coupler.IO doesn’t support timezone or FX normalization natively—handled manually in Python
- Power BI is used for final transformation to reduce warehouse processing cost and leverage pre-existing templates
- The system is optimized for daily FB account onboarding by non-technical admins


