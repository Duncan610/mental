# MentalHealth
### A Population-Level Mental Health Early Warning System

[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1-FFC832?logo=duckdb)](https://duckdb.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://www.python.org/)
[![dlt](https://img.shields.io/badge/dlt-1.4-7E57C2)](https://dlthub.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-22C55E)](LICENSE)
[![Status](https://img.shields.io/badge/Milestone-1%20of%203%20Complete-blue)]()

---

## The Story Behind This Project

Growing up in Kenya, mental health was not something people talked about.

It was not cruelty or ignorance; it was just the world we lived in. When someone grew quiet and withdrawn, family members called it stress, or tiredness, or spiritual weakness. When someone stopped eating, stopped leaving their room, stopped responding to their name, people prayed. They gave advice. They waited for it to pass.

Nobody called it depression. Nobody called a hotline. There was no data being collected, no system raising a flag, no algorithm anywhere in the world saying: *this community is struggling, and we need to act now.*

The suffering was real. The silence around it was louder.

When I began my journey into data engineering, I kept returning to one question that wouldn't leave me alone: **what if we had seen it coming?**

What if there had been a system, not a perfect system, not an invasive one, but a quiet, data-driven one, watching the signals that show up weeks before a crisis peaks? The shift in the language people use online when they're in pain. The winter when daylight hours drop and overdose calls spike. The month the factory closes, and suddenly, the mental health ward is overwhelmed. These signals exist. They precede crises. And in many places, nobody is connecting them.

That question became this project.

---

## The Professional Problem

The data gap I grew up inside is not unique to Africa. It exists in the United States. It exists everywhere.

Mental health crises suicide spikes, opioid overdose waves, and acute anxiety surges do not appear suddenly. They build. Research shows that population-level distress signals appear **2 to 4 weeks before** a crisis peaks in emergency rooms and on crisis hotlines. Social media language shifts. Unemployment rises. Daylight hours shorten. Drug overdose mortality trends change.

The problem is that **no one is connecting these signals in real time.**

Health systems, insurance companies, and government agencies have fragments of this data sitting in separate silos. CDC mortality records here. Bureau of Labor Statistics data there. Weather observation files nobody queries. Social media posts that vanish. The signals exist, they're just never assembled into something a public health team can actually act on.

> *According to the CDC, over 49,000 Americans died by suicide in 2022, the highest number ever recorded. Drug overdose deaths exceeded 107,000 in the same year. These are not random events. They have patterns. Patterns have data. Data can be engineered.*

I am an entry-level analytics engineer building toward a career where data infrastructure serves something meaningful. This project is my attempt to demonstrate that technically, and honestly.

---

## What MentalHealthPulse Does

**MentalHealthPulse** is an analytics engineering pipeline that ingests data from five live public sources, transforms it through a production-grade dbt layer, and produces a county-level mental health crisis risk score updated daily.

It is the kind of system that teams at **UnitedHealth Group, Optum, CVS Aetna, Spring Health, and SAMHSA** are actively spending millions to build internally. This is a portfolio-scale version of that engineered with the same principles, minus the budget.

### What gets ingested

| # | Source | What it captures | Frequency |
|---|---|---|---|
| 1 | **Bluesky AT Protocol** | Mental health discourse — language, distress signals, community sentiment | Daily |
| 2 | **CDC WONDER / data.cdc.gov** | Drug overdose and suicide mortality by county | Monthly |
| 3 | **NOAA Climate Data** | Temperature, daylight hours, extreme weather events | Daily |
| 4 | **BLS (Bureau of Labor Statistics)** | Unemployment rates and mass layoff events by state | Monthly |
| 5 | **SAMHSA NSDUH** | Mental illness prevalence, treatment gap, baseline vulnerability by state | Annual |

### What gets built

- A **staging layer** — clean, typed, documented, tested raw data per source
- An **intermediate layer** — NLP sentiment scoring, cross-source county joins, feature engineering
- A **marts layer** — a county-level daily crisis risk score, trend signals, BI-ready tables

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (5)                        │
│                                                              │
│  Bluesky API   │  CDC API    │  NOAA API  │  BLS API        │
│  (mental health│  (overdose  │  (weather, │  (unemployment,  │
│   hashtags,    │   & suicide │   daylight │   layoff        │
│   keywords)    │   mortality)│   hours)   │   events)       │
│                                                              │
│                       SAMHSA NSDUH                           │
│                  (state mental health surveys)               │
└────────────────────────────┬─────────────────────────────────┘
                             │  dlt (data load tool)
                             │  incremental · idempotent · validated
                             ▼
┌──────────────────────────────────────────────────────────────┐
│               DUCKDB  —  mental_health_pulse.duckdb          │
│                                                              │
│  raw_bluesky │ raw_cdc │ raw_noaa │ raw_bls │ raw_samhsa    │
└────────────────────────────┬─────────────────────────────────┘
                             │  dbt-duckdb
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                    STAGING  (stg_*)                          │
│   Cleaned · typed · renamed · deduplicated · tested          │
│   One model per source. No joins. No business logic.         │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                 INTERMEDIATE  (int_*)                        │
│   Sentiment scoring · county joins · feature engineering     │
│   Lagged economic indicators · SAD daylight index            │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                     MARTS  (mart_*)                          │
│   county_crisis_risk_daily  ·  sentiment_trends              │
│   mortality_signals  ·  economic_stress_index                │
└──────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
mental_health_pulse/
├── pipeline/                      # Data ingestion (dlt-based)
│   ├── bluesky_pipeline.py        # Bluesky → DuckDB (AT Protocol)
│   ├── cdc_pipeline.py            # CDC WONDER → DuckDB
│   ├── other_pipelines.py         # NOAA + BLS + SAMHSA → DuckDB
│   └── run_all.py                 # Master orchestrator (circuit breaker)
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/               
│   │   │   ├── sources/sources.yml
│   │   │   ├── stg_bluesky__posts.sql
│   │   │   ├── stg_cdc__mortality.sql
│   │   │   ├── stg_noaa__weather.sql
│   │   │   ├── stg_bls__economics.sql
│   │   │   ├── stg_samhsa__surveys.sql
│   │   │   └── staging.yml        # Column tests + documentation
│   │   ├── intermediate/      
│   │   └── marts/              
│   ├── macros/                    # generate_schema_name, safe_divide, sentiment_bucket
│   ├── tests/                     # Custom singular tests
│   └── dbt_project.yml
│
├── config/
│   ├── profiles.yml               # DuckDB connection (dev/ci/prod)
│   └── settings.py                # Pydantic settings (env-driven)
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── .github/workflows/             # CI/CD: dbt test on every PR
├── .env.example
├── requirements.txt
└── README.md
```

---

## Getting Started

This project runs entirely locally. No cloud account. No credit card. No Snowflake trial.

### What you need

| Credential | Where to get it | Time |
|---|---|---|
| Bluesky App Password | [bsky.app](https://bsky.app)
| NOAA API Token | [ncdc.noaa.gov/cdo-web/token](https://www.ncdc.noaa.gov/cdo-web/token)
| BLS API Key | [data.bls.gov/registrationEngine](https://data.bls.gov/registrationEngine/) 
| CDC API | None required | — |
| SAMHSA data | None required | — |



## Engineering Decisions

> *Every tool and concept in this project was chosen deliberately. This section exists because "I used dbt and Snowflake" tells you nothing — but "here's why I chose each tool and what I would have used instead" tells you everything.*

---

### 1. Domain — Public Mental Health

**Why this domain:** Mental health data engineering is a $50B+ problem space actively worked on by UnitedHealth, Optum, CVS Aetna, Spring Health, Cerebral, and government bodies including SAMHSA and the CDC. Choosing this domain signals genuine domain awareness, not just technical proficiency.

It is also personal. I grew up in a place where mental health was never discussed, where the stigma was total and the data infrastructure was nonexistent. This project is partly my answer to that silence.

*Built by an entry-level analytics engineer who believes that where you start doesn't determine what you can build.*
*Questions, feedback, or opportunities — find me on [LinkedIn](https://linkedin.com/in/yourprofile).*
