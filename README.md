# Car Characteristics Scraper for Demand Estimation

This repository contains a Python script that scrapes detailed car specifications from [GuideAutoWeb](https://www.guideautoweb.com). The data is structured by make, model, year, and trim, and includes key characteristics used in car demand estimation models.

pip install pandas requests beautifulsoup4
python scrape_car_specs.py

## 🧠 Purpose

This dataset supports empirical research and modeling in:
- Vehicle demand estimation (e.g., logit, nested logit)
- EV vs ICE market comparisons
- Structural industrial organization models

## 📦 Features

- Extracts data from 2012 to 2025 (configurable)
- Covers 80+ makes and hundreds of models
- Captures:
  - `Make`, `Model`, `Year`, `Trim`
  - `MSRP`, `Engine`, `Power`, `Torque`
  - `Fuel_Cost` (0 if Electric), `Weight`, `Category`, `Vehicle Type`
- Skips invalid or incomplete entries
- Saves a unified SQLite database

## 💾 Output

A single SQLite database saved at:


Main table: `Car_Guide`

Example query:
```sql
SELECT * FROM Car_Guide WHERE LOWER(Engine) LIKE '%electric%';


👤 Author
Hugo Cordeau
PhD Candidate in Economics
Specializing in sustainable transportation and demand modeling
