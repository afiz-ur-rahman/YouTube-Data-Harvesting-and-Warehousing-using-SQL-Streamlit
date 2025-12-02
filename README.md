# YouTube-Data-Harvesting-and-Warehousing-using-SQL-Streamlit

This project demonstrates an end-to-end data engineering and analytics pipeline for YouTube channel insights. It automatically harvests data from the YouTube Data API, processes and stores it in a structured SQL data warehouse, and provides an interactive Streamlit dashboard for analysis.
The system enables users to input any YouTube Channel ID and instantly explore:

Channel-level metadata

Video details (title, description, publish date)

Engagement metrics (views, likes, comments)

Publication trends and analytics

Historical and incremental updates for data warehousing

Built using Python, SQL, and Streamlit, this project follows standard ETL practices — Extract, Transform, and Load — to maintain a clean analytical dataset suitable for reporting, insights generation, and future machine-learning workflows.

Key Features
 1. YouTube Data Harvesting

Fetch videos and channel details using the YouTube Data API.

Automatically gather statistics: views, likes, dislikes, comments.

Extract and store data in raw and cleaned formats.

 2. Data Warehousing (SQL)

Data is stored in a normalized SQL schema.

Includes tables for channels, videos, and historical snapshots.

Supports upsert logic to avoid duplicate entries.

 3. ETL Pipeline (Python)

Extract: Collects data from API (or other sources like pytube/youtube-dl).

Transform: Cleans, validates, and converts data to warehouse format.

Load: Inserts processed data into SQL with indexing for fast queries.

 4. Streamlit Dashboard

User-friendly interface to search any YouTube Channel ID.

Displays tables, charts, metrics, and trends.

Interactive visualizations using Streamlit components.

 5. Scalable Architecture

Handles multiple channel IDs.

Can run via scheduler (Airflow/cron/GitHub Actions).

Supports Docker deployment and Cloud SQL.

Tech Stack

Python (Streamlit, Pandas, SQLAlchemy, Requests)

SQL Database: PostgreSQL / MySQL / TiDB

YouTube Data API v3

Docker (optional)

GitHub Actions for CI/CD (optional)

 Use Cases

Social media analytics dashboards

Competitor channel monitoring

Marketing & SEO analysis

Data engineering portfolio project

Academic or certification submissions
