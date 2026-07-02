# 📊 Vietnam Recruitment Market Intelligence Pipeline (2025–2026)

## 📌 1. Project Overview & Objectives

**Domain:** Data Engineering / Recruitment Analytics / Labor Market Intelligence

### Objective

This project aims to build a complete **end-to-end recruitment analytics platform** for the Vietnamese job market, focusing on:

* Large-scale job data collection
* ETL pipeline engineering
* Data standardization for highly unstructured recruitment data
* Analytical Data Warehouse design
* Labor market intelligence & salary analytics

The system transforms raw job postings from multiple recruitment platforms into a clean, analytics-ready warehouse powering Power BI dashboards and market insights.

---

## 📦 2. Data Scope

### Timeframe

* **April 2025 – February 2026**

### Scale

* **54,532 hiring demands**
* **29,909 unique job postings**
* **10,107 companies**
* **10 recruitment platforms**

### Data Sources

* TopCV
* VietnamWorks
* ITviec
* LinkedIn Vietnam
* CareerLink
* JobStreet
* TimViecNhanh
* and other Vietnamese recruitment platforms

---

## ⚠️ 3. Core Technical Challenges

The project focuses heavily on solving real-world data engineering problems caused by noisy and unstructured recruitment data.

### Key Challenges

#### 1. Highly Unstructured Text Data

Critical fields such as salary, experience, location, company name, skills, and education were stored as inconsistent free-text formats across platforms.

```
"15 - 20 triệu"
"Up to $2000"
"Không yêu cầu kinh nghiệm"
"Hà Nội, TP.HCM"
"CTCP ABC Việt Nam"
```

#### 2. Company Name Deduplication

Company names appeared in thousands of inconsistent formats — legal prefixes, abbreviations, English/Vietnamese variants, and spelling inconsistencies. The challenge was entity resolution at scale, not just exact matching.

#### 3. Multi-Location Recruitment Posts

One job posting could target multiple provinces simultaneously, requiring location normalization, geographic mapping, and row fan-out logic to preserve analytical accuracy.

#### 4. Large-Scale ETL Reliability

The pipeline needed to support incremental daily processing, full reprocessing, automated orchestration, error tracking, and warehouse synchronization — while maintaining low parsing failure rates.

---

## 🛠️ 4. Tech Stack & Tools

| Layer | Technologies |
| --- | --- |
| Crawling | Scrapy, Scrapy-Playwright, Selenium |
| ETL | Python, Pandas, Regex, RapidFuzz |
| AI Parsing | Groq API (llama-3.3-70b-versatile) |
| Database | PostgreSQL |
| Data Warehouse | Star Schema + Stored Procedures |
| Visualization | Power BI |
| Orchestration | Windows Task Scheduler + Batch Script |

---

## ⚙️ 5. Setup & Installation

> **Note:** This pipeline is designed and tested on **Windows only**.

### Prerequisites

| Requirement | Version |
| --- | --- |
| Python | 3.10+ |
| PostgreSQL | 14+ |
| Google Chrome | Latest |

### Step 1 — Clone Repository

```bash
git clone https://github.com/anhthaojb/IT_VN_recruit.git
cd IT_VN_recruit
```

### Step 2 — Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Then install Playwright browsers:

```bash
playwright install chromium
```

### Step 3 — Configure Environment Variables

Create a `.env` file in the root directory:

```bash
copy jobscrapers\.env.example jobscrapers\.env
```

Fill in your credentials:

```env
# LinkedIn crawler credentials
LINKEDIN_USERNAME=your_email@gmail.com
LINKEDIN_PASSWORD=your_password

# ITviec crawler credentials
ITVIEC_EMAIL=your_email@gmail.com
ITVIEC_PASSWORD=your_password

# Groq API — get a free key at https://console.groq.com
GROQ_API_KEY=gsk_...

# PostgreSQL connection
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/recruitment_dw
DB_PASSWORD=your_password
```

### Step 4 — Set Up PostgreSQL Database

Create the database, then run the schema scripts to initialize all tables and stored procedures:

```bash
# Create database (run once)
psql -U postgres -c "CREATE DATABASE recruitment_dw;"

# Apply schema (tables + stored procedures)
psql -U postgres -d recruitment_dw -f sql/schema.sql
psql -U postgres -d recruitment_dw -f sql/sp_etl_load_dw.sql
```

Or open the files in **pgAdmin** and execute them directly:
- `sql/schema.sql` — tạo toàn bộ bảng và index
- `sql/sp_etl_load_dw.sql` — tạo stored procedure ETL load vào Data Warehouse

### Step 5 — Run the Pipeline

**Daily mode** — incremental crawl và ETL:

```bash
run.bat daily
```

**Full mode** — recrawl toàn bộ:

```bash
run.bat full
```

**ETL only** — transform và load, không crawl:

```bash
python jobscrapers/transform.py
```

### Step 6 — (Optional) Automate with Windows Task Scheduler

1. Open **Task Scheduler** → *Create Basic Task*
2. Set trigger: **Daily** at your preferred time (e.g. 2:00 AM)
3. Set action: **Start a program**
   * Program: `C:\path\to\IT_VN_recruit\run.bat`
   * Arguments: `daily`
   * Start in: `C:\path\to\IT_VN_recruit`

---

## 🔄 6. Data Pipeline & ETL Architecture

### End-to-End Pipeline

```
Scrapy / Scrapy-Playwright / Selenium Crawlers
                    ↓
           Raw PostgreSQL Tables
                    ↓
           ETL (transform.py)
                    ↓
      Data Cleaning & Parsing
                    ↓
      Company Deduplication
                    ↓
         Groq AI JD Parsing
                    ↓
          fact_jobs_etl
                    ↓
      Stored Procedure ETL
                    ↓
      Star Schema Warehouse
                    ↓
       Power BI Dashboards
```

### Pipeline Modes

```
MODE=daily  → crawl & process new data only
MODE=full   → full recrawl + full reprocess
```

### Crawling Architecture

The system uses a hybrid crawling approach:

| Method | Use Case |
| --- | --- |
| Scrapy | Static HTML job boards |
| Scrapy-Playwright | Heavy JS rendering, async loading, anti-bot handling |
| Selenium | Platforms requiring browser automation and login |
| API Requests | Hidden/internal APIs for structured job data |

Each job posting includes 24 raw attributes: job title, company, salary, experience, location, skills, education, work mode, job description, and metadata timestamps.

### ETL & Data Transformation

| Component | Description |
| --- | --- |
| Salary Parsing | Normalizes VND/USD ranges, shorthand units (k, triệu, M), negotiable |
| Experience Extraction | Regex + fallback logic for years, ranges, fresher detection |
| Company Deduplication | Canonical normalization + RapidFuzz fuzzy matching |
| AI JD Parsing | Groq API extracts skills and requirements from unstructured descriptions |
| Geographic Fan-Out | 1 multi-location posting → multiple analytical rows |

### ETL Output Tables

| Table | Purpose |
| --- | --- |
| `fact_jobs_etl` | Cleaned analytical staging table |
| `fact_etl_log` | Pipeline execution logs |
| `fact_etl_error` | Parsing errors for debugging |

**Result:** Processed 48K+ rows with parsing error rate below 0.02%.

---

## 🗄️ 7. Data Warehouse Design

### Star Schema Architecture

```
Dim_Company
Dim_Location
Dim_Industry
Dim_JobCategory
Dim_Level
Dim_WorkMode
Dim_Source
        ↓
Fact_JobPostings
        ↓
bridge_jobrequire
        ↓
Dim_Require
```

`Fact_JobPostings` stores salary metrics, experience ranges, posting dates, normalized classifications, and dimensional foreign keys. Skills and certifications are modeled in `Dim_Require` + `bridge_jobrequire` for many-to-many analytical relationships.

---

## 📊 8. Business Intelligence & Market Insights

### Layer 1: Recruitment Market Overview

![Overview Dashboard](visuals/overview.png)

* Hanoi and Ho Chi Minh City contributed over **83% of total hiring demand**
* Recruitment demand peaked during **Q4/2025**
* Technology, Finance, and Manufacturing showed comparable hiring scale

### Layer 2: Skill Demand & Technology Landscape

![Skill Dashboard](visuals/skill.png)

* SQL appeared more frequently than Python across Data-related roles
* Communication and English were among the most requested skills
* Docker and CI/CD increasingly appeared in Data Engineering positions

### Layer 3: Salary & Career Analytics

![Salary Dashboard](visuals/role.png)

* Data Engineer roles had the highest entry salary
* Senior Data Analyst salaries often exceeded Data Scientist salaries
* Salary growth accelerated significantly after 3–5 years of experience

---

## 🚀 9. Key Engineering Achievements

### Data Engineering

* Built a production-style ETL pipeline for highly unstructured recruitment data
* Automated daily crawling, transformation, and warehouse loading
* Implemented company entity resolution using RapidFuzz fuzzy matching at scale
* Integrated Groq AI for parsing unstructured job descriptions

### Data Warehouse

* Designed analytical Star Schema optimized for Power BI
* Built incremental ETL stored procedures with referential integrity

### Analytics

* Delivered market intelligence dashboards covering salary trends, hiring demand, skill demand, geographic analysis, and Data role benchmarking

---

## 📂 10. Project Structure

```
IT_VN_recruit/
│
├── run.bat                    # Main orchestration script
├── run_spiders.py             # Spider runner
├── scrapy.cfg
├── requirements.txt
├── company.csv
│
├── jobscrapers/               # Scrapy project
│   ├── __init__.py
│   ├── transform.py           # ETL core
│   ├── dedup.py               # Deduplication logic
│   ├── items.py
│   ├── lookups.py             # Normalization dictionaries
│   ├── middlewares.py
│   ├── pipelines.py
│   ├── settings.py
│   ├── .env.example           # Environment variable template
│   │
│   └── spiders/
│       ├── careerlink.py
│       ├── careersviet.py
│       ├── itviec_selenium.py
│       ├── joboko.py
│       ├── jobsgo.py
│       ├── linkedin_selenium.py
│       ├── timviec365.py
│       ├── topcv.py
│       ├── vieclam24h.py
│       └── vietnamwork.py
│
├── sql/
│   ├── schema.sql             # Database schema (tables, indexes)
│   └── sp_etl_load_dw.sql    # Stored procedure ETL → Data Warehouse
│
└── visuals/
    ├── dataful.png
    ├── datastructure.png
    ├── detail.png
    ├── overview.png
    ├── role.png
    └── skill.png
```
