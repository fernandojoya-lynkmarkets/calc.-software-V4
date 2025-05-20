# Calc Software V4

This repository contains a Python utility for calculating ETN NAV values from pasted custodian balance data.

## Usage

Paste a table of daily balances into the script via `stdin`:

```bash
python etn_nav_calculator.py < balances.txt
```

The script writes the results into `etn_nav.db` (SQLite) and prints the calculated table to the console.

## ETN Series Terms Web App

Install dependencies:

```bash
pip install streamlit pandas
```

Start the app:

```bash
streamlit run etn_series_app.py
```

