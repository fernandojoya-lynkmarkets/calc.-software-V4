import sqlite3
import pandas as pd
import streamlit as st

DB_NAME = 'etn_series.db'


def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS series_terms (
                isin TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                management_fee TEXT NOT NULL,
                maintenance_fee TEXT NOT NULL
            )"""
    )
    # insert example if table empty
    c.execute("SELECT COUNT(*) FROM series_terms")
    count = c.fetchone()[0]
    if count == 0:
        c.execute(
            "INSERT OR IGNORE INTO series_terms (isin, name, management_fee, maintenance_fee) VALUES (?, ?, ?, ?)",
            ("XS00000000", "Quantum Innovation Fund", "2.00%", "0.25%"),
        )
    conn.commit()
    conn.close()


def fetch_series():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM series_terms", conn)
    conn.close()
    return df


def create_series(name: str, isin: str, management_fee: str, maintenance_fee: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO series_terms (isin, name, management_fee, maintenance_fee) VALUES (?, ?, ?, ?)",
        (isin, name, management_fee, maintenance_fee),
    )
    conn.commit()
    conn.close()


def update_series(isin: str, name: str, management_fee: str, maintenance_fee: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "UPDATE series_terms SET name=?, management_fee=?, maintenance_fee=? WHERE isin=?",
        (name, management_fee, maintenance_fee, isin),
    )
    conn.commit()
    conn.close()


def delete_series(isin: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM series_terms WHERE isin=?", (isin,))
    conn.commit()
    conn.close()


# Streamlit Interface
st.title("ETN Series Terms Manager")
init_db()

st.subheader("View All Series")
series_df = fetch_series()
st.dataframe(series_df, hide_index=True)

st.subheader("Add New Series")
with st.form("add_form"):
    add_name = st.text_input("Name")
    add_isin = st.text_input("ISIN")
    add_mfee = st.text_input("Management Fee", value="")
    add_maintfee = st.text_input("Maintenance Fee", value="")
    submit_add = st.form_submit_button("Add Series")

if submit_add:
    try:
        create_series(add_name, add_isin, add_mfee, add_maintfee)
        st.success(f"Series {add_name} created.")
        st.experimental_rerun()
    except sqlite3.IntegrityError:
        st.error("ISIN already exists.")

st.subheader("Edit or Delete Series")
edit_df = fetch_series()
if not edit_df.empty:
    selected_isin = st.selectbox("Select Series", edit_df["isin"].tolist())
    selected_row = edit_df[edit_df["isin"] == selected_isin].iloc[0]

    with st.form("edit_form"):
        edit_name = st.text_input("Name", value=selected_row["name"])
        st.text_input("ISIN", value=selected_isin, disabled=True)
        edit_mfee = st.text_input("Management Fee", value=selected_row["management_fee"])
        edit_maintfee = st.text_input("Maintenance Fee", value=selected_row["maintenance_fee"])
        col1, col2 = st.columns(2)
        update_btn = col1.form_submit_button("Update Series")
        delete_btn = col2.form_submit_button("Delete Series")

    if update_btn:
        update_series(selected_isin, edit_name, edit_mfee, edit_maintfee)
        st.success("Series updated.")
        st.experimental_rerun()
    if delete_btn:
        delete_series(selected_isin)
        st.success("Series deleted.")
        st.experimental_rerun()
else:
    st.info("No series available.")
