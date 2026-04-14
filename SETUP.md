# Setup

## Prerequisites

- Python 3.10+

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Run the pipeline

No credentials needed. Everything runs locally.

```bash
cd deel_dbt
dbt deps
dbt build
```

A `deel.duckdb` file is created in the project root with all tables ready to query.

## 3. Generate docs

```bash
cd deel_dbt
dbt docs generate && dbt docs serve
```
