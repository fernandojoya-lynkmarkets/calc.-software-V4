import sys
from io import StringIO
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine, text


class ETNNAVCalculator:
    """Calculate and store ETN NAV data."""

    MGMT_RATE = 0.02 / 365
    MAINT_RATE = 0.0035 / 365

    def __init__(self, df, db_url="sqlite:///etn_nav.db"):
        self.df = df.copy()
        self.engine = create_engine(db_url)

    def _validate_dates(self):
        dates = self.df["Date"].dt.date
        for prev, curr in zip(dates[:-1], dates[1:]):
            if curr != prev + timedelta(days=1):
                raise ValueError(
                    f"Dates must be consecutive business days: {prev} to {curr}"
                )

    def calculate(self):
        self._validate_dates()
        accrued = 0.0
        gross_nav = []
        mgmt_fee = []
        maint_fee = []
        total_fee = []
        accrued_fees = []
        net_cav = []
        net_nav = []
        pct_change = []
        prev_net_nav = None

        for _, row in self.df.iterrows():
            bal = row["Total_Balance"]
            notes = row["Initial_Nominal_Notes"]

            m_fee = self.MGMT_RATE * bal
            c_fee = self.MAINT_RATE * bal
            t_fee = m_fee + c_fee

            accrued += t_fee
            net = bal - accrued

            if notes:
                g_nav = bal / notes * 100.0
                n_nav = net / notes * 100.0
            else:
                g_nav = 100.0
                n_nav = 100.0

            if prev_net_nav is None:
                pct = 0.0
            else:
                pct = (n_nav - prev_net_nav) / prev_net_nav * 100.0
            prev_net_nav = n_nav

            gross_nav.append(g_nav)
            mgmt_fee.append(m_fee)
            maint_fee.append(c_fee)
            total_fee.append(t_fee)
            accrued_fees.append(accrued)
            net_cav.append(net)
            net_nav.append(n_nav)
            pct_change.append(pct)

        self.df = self.df.assign(
            gross_nav=gross_nav,
            mgmt_fee=mgmt_fee,
            maint_fee=maint_fee,
            total_fees=total_fee,
            accrued_fees=accrued_fees,
            net_cav=net_cav,
            net_nav=net_nav,
            pct_change=pct_change,
        )
        return self.df

    def write_to_db(self):
        create_sql = text(
            """
        CREATE TABLE IF NOT EXISTS etn_nav (
            date DATE PRIMARY KEY,
            gross_nav REAL,
            mgmt_fee REAL,
            maint_fee REAL,
            total_fees REAL,
            accrued_fees REAL,
            net_cav REAL,
            net_nav REAL,
            pct_change REAL
        );
        """
        )

        insert_sql = text(
            """
        INSERT INTO etn_nav(date, gross_nav, mgmt_fee, maint_fee, total_fees,
            accrued_fees, net_cav, net_nav, pct_change)
        VALUES(:date, :gross_nav, :mgmt_fee, :maint_fee, :total_fees,
            :accrued_fees, :net_cav, :net_nav, :pct_change)
        ON CONFLICT(date) DO UPDATE SET
            gross_nav=excluded.gross_nav,
            mgmt_fee=excluded.mgmt_fee,
            maint_fee=excluded.maint_fee,
            total_fees=excluded.total_fees,
            accrued_fees=excluded.accrued_fees,
            net_cav=excluded.net_cav,
            net_nav=excluded.net_nav,
            pct_change=excluded.pct_change;
        """
        )

        with self.engine.begin() as conn:
            conn.execute(create_sql)
            for _, row in self.df.iterrows():
                conn.execute(
                    insert_sql,
                    {
                        "date": row["Date"].date(),
                        "gross_nav": row["gross_nav"],
                        "mgmt_fee": row["mgmt_fee"],
                        "maint_fee": row["maint_fee"],
                        "total_fees": row["total_fees"],
                        "accrued_fees": row["accrued_fees"],
                        "net_cav": row["net_cav"],
                        "net_nav": row["net_nav"],
                        "pct_change": row["pct_change"],
                    },
                )


def main():
    text = sys.stdin.read()
    if not text.strip():
        print("No input provided")
        return

    df = pd.read_csv(StringIO(text), delim_whitespace=True, parse_dates=["Date"])

    calc = ETNNAVCalculator(df)
    result_df = calc.calculate()
    calc.write_to_db()
    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()
