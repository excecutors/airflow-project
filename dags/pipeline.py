from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

import pandas as pd
from faker import Faker
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------
# Config (paths & DB)
# ---------------------------------------------------------------------
BASE_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(BASE_DIR, "raw")
STAGE_DIR = os.path.join(BASE_DIR, "stage")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGE_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Inside Docker, Postgres is 'db:5432' with user/pass from .env
PG_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@db:5432/airflow"
)

TABLE_NAME = "people_companies"

default_args = {
    "owner": "freddy",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
with DAG(
    dag_id="people_company_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="0 2 * * *",   # daily at 2:00 AM
    catchup=False,
    description="Parallel ingest -> transform -> merge -> load -> analyze -> cleanup",
    tags=["homework", "parallel", "pandas"],
) as dag:

    @task
    def clear_folder(folder_path: str = BASE_DIR) -> str:
        """
        Deletes everything in data/raw and data/stage (keeps reports).
        Returns the base path (for XCom chaining).
        """
        for sub in ("raw", "stage"):
            p = os.path.join(folder_path, sub)
            if os.path.exists(p):
                for fn in os.listdir(p):
                    fp = os.path.join(p, fn)
                    try:
                        if os.path.isfile(fp) or os.path.islink(fp):
                            os.remove(fp)
                        elif os.path.isdir(fp):
                            import shutil
                            shutil.rmtree(fp)
                    except Exception as e:
                        print(f"Failed to delete {fp}: {e}")
        return folder_path

    @task_group(group_id="ingest")
    def ingest_group():
        fake = Faker()

        @task
        def fetch_companies(n_companies: int = 30) -> str:
            """
            Generate a companies CSV with sector labels.
            """
            rng = np.random.default_rng(42)
            sectors = ["Tech", "Finance", "Retail", "Energy", "Health"]
            companies = []
            for i in range(1, n_companies + 1):
                companies.append({
                    "company_id": i,
                    "company_name": f"{fake.company()}",
                    "sector": rng.choice(sectors).item()
                })
            dfc = pd.DataFrame(companies)
            path = os.path.join(RAW_DIR, "companies.csv")
            dfc.to_csv(path, index=False)
            return path

        @task
        def fetch_persons(n_people: int = 200, max_company_id: int = 30) -> str:
            """
            Generate a persons CSV referencing company_id.
            """
            rng = np.random.default_rng(123)
            persons = []
            for _ in range(n_people):
                cid = int(rng.integers(1, max_company_id + 1))
                persons.append({
                    "person_id": fake.unique.random_int(min=1, max=10_000_000),
                    "full_name": fake.name(),
                    "email": fake.email(),
                    "salary": float(max(35_000, min(250_000, rng.normal(95_000, 25_000)))),
                    "active": bool(rng.choice([True, False], p=[0.85, 0.15])),
                    "company_id": cid,
                })
            dfp = pd.DataFrame(persons)
            path = os.path.join(RAW_DIR, "persons.csv")
            dfp.to_csv(path, index=False)
            return path

        return fetch_companies(), fetch_persons()

    @task_group(group_id="transform_merge")
    def transform_group(companies_path: str, persons_path: str) -> str:

        @task
        def clean_companies(path: str) -> str:
            df = pd.read_csv(path)
            # dedupe on company_id
            df = df.drop_duplicates(subset=["company_id"])
            out = os.path.join(STAGE_DIR, "companies_clean.csv")
            df.to_csv(out, index=False)
            return out

        @task
        def clean_persons(path: str) -> str:
            df = pd.read_csv(path)

            # normalize emails
            df["email"] = df["email"].str.strip().str.lower()

            # clamp salaries and drop inactive
            df["salary"] = df["salary"].clip(lower=35000, upper=250000)
            df = df[df["active"]]

            # dedupe on person_id (keep first)
            df = df.drop_duplicates(subset=["person_id"])

            out = os.path.join(STAGE_DIR, "persons_clean.csv")
            df.to_csv(out, index=False)
            return out

        @task
        def merge_people_companies(companies_clean_path: str, persons_clean_path: str) -> str:
            dc = pd.read_csv(companies_clean_path)
            dp = pd.read_csv(persons_clean_path)

            merged = dp.merge(dc, on="company_id", how="inner")
            merged["email_domain"] = merged["email"].str.split("@").str[-1]
            merged["salary_k"] = (merged["salary"] / 1000).round(1)

            out = os.path.join(STAGE_DIR, "people_companies.csv")
            merged.to_csv(out, index=False)
            return out

        cc = clean_companies(companies_path)
        cp = clean_persons(persons_path)
        merged_path = merge_people_companies(cc, cp)
        return merged_path

    @task
    def load_to_postgres(csv_path: str, table: str = TABLE_NAME) -> str:
        """
        Load merged CSV to Postgres (replace table).
        """
        engine = create_engine(PG_URL)
        df = pd.read_csv(csv_path)

        with engine.begin() as conn:
            # Create schema/table if not exists; then replace
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS public.{table} (
                    person_id BIGINT,
                    full_name TEXT,
                    email TEXT,
                    salary DOUBLE PRECISION,
                    active BOOLEAN,
                    company_id INT,
                    company_name TEXT,
                    sector TEXT,
                    email_domain TEXT,
                    salary_k DOUBLE PRECISION
                );
            """))
            # replace table
            conn.execute(text(f"TRUNCATE TABLE public.{table};"))
        # Use pandas to_sql (will append into the truncated table)
        df.to_sql(table, engine, schema="public", if_exists="append", index=False)
        return table

    @task
    def analyze_and_plot(table: str, out_dir: str = REPORTS_DIR) -> str:
        """
        Read from Postgres and save a matplotlib figure.
        """
        engine = create_engine(PG_URL)
        q = f"""
        SELECT sector, ROUND(AVG(salary)::numeric, 2) AS avg_salary
        FROM public.{table}
        GROUP BY sector
        ORDER BY sector;
        """
        pdf = pd.read_sql(q, engine)

        fig_path = os.path.join(out_dir, "avg_salary_by_sector.png")
        # One simple bar chart (Airflow image-friendly)
        plt.figure(figsize=(6,4))
        plt.bar(pdf["sector"], pdf["avg_salary"])
        plt.title("Average Salary by Sector")
        plt.xlabel("Sector")
        plt.ylabel("Average Salary (USD)")
        plt.tight_layout()
        plt.savefig(fig_path)
        plt.close()
        return fig_path

    @task
    def cleanup_intermediates(base_dir: str = BASE_DIR) -> str:
        """
        Delete raw & stage; keep final reports.
        """
        # Reuse clear_folder’s logic by calling it here quickly
        for sub in ("raw", "stage"):
            p = os.path.join(base_dir, sub)
            if os.path.exists(p):
                for fn in os.listdir(p):
                    fp = os.path.join(p, fn)
                    try:
                        if os.path.isfile(fp) or os.path.islink(fp):
                            os.remove(fp)
                        elif os.path.isdir(fp):
                            import shutil
                            shutil.rmtree(fp)
                    except Exception as e:
                        print(f"Failed to delete {fp}: {e}")
        return base_dir

    # -------------------------
    # Orchestration
    # -------------------------
    start = clear_folder()
    c_path, p_path = ingest_group()
    merged = transform_group(c_path, p_path)
    loaded = load_to_postgres(merged)
    report = analyze_and_plot(loaded)
    done = cleanup_intermediates()

    chain(start, [c_path, p_path], merged, loaded, report, done)