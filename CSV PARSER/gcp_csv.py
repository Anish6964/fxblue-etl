import os
import logging
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import hashlib
import psycopg2
from google.cloud import storage

# ─── Configuration via ENV VARs ─────────────────────────────────────────────
# Database creds now via socket
DB_NAME                    = os.environ["DB_NAME"]
DB_USER                    = os.environ["DB_USER"]
DB_PASSWORD                = os.environ["DB_PASSWORD"]
CLOUD_SQL_CONNECTION_NAME  = os.environ["CLOUD_SQL_CONNECTION_NAME"]

BUCKET_NAME = os.environ["BUCKET_NAME"]
CSV_PREFIX  = os.environ.get("CSV_PREFIX", "testcsvs/")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 10))

# ─── Logging Setup ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Database Utility ──────────────────────────────────────────────────────
def get_db_conn():
    # connect via Cloud SQL socket
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}"
    )


UPSERT_SQL = """
INSERT INTO historical_trades (
    ticket, account_id, symbol, trade_type, entry_price, exit_price,
    timestamp, lot_size, pnl, net_profit, mae, mfe, pips, tp, sl,
    trade_duration_hours,
    gpt_inferred_strategy, gpt_strategy_confidence,
    gpt_trade_evaluation, gpt_alternative_action,
    was_gpt_recommendation_followed, gpt_impact_alignment
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (ticket) DO UPDATE SET
    account_id            = EXCLUDED.account_id,
    symbol                = EXCLUDED.symbol,
    trade_type            = EXCLUDED.trade_type,
    entry_price           = EXCLUDED.entry_price,
    exit_price            = EXCLUDED.exit_price,
    timestamp             = EXCLUDED.timestamp,
    lot_size              = EXCLUDED.lot_size,
    pnl                   = EXCLUDED.pnl,
    net_profit            = EXCLUDED.net_profit,
    mae                   = EXCLUDED.mae,
    mfe                   = EXCLUDED.mfe,
    pips                  = EXCLUDED.pips,
    tp                    = EXCLUDED.tp,
    sl                    = EXCLUDED.sl,
    trade_duration_hours  = EXCLUDED.trade_duration_hours
"""


# ─── Core Processing Function ───────────────────────────────────────────────
def process_blob(blob):
    """Download, clean, and upsert one CSV blob.  Logs and skips on error."""
    try:
        logging.info(f"Starting {blob.name}")
        raw = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(raw), skiprows=1)

        # Basic renames + account_id
        account_id = os.path.basename(blob.name).split(".csv")[0]
        df["account_id"] = account_id
        df.rename(
            columns={
                "Symbol": "symbol",
                "Buy/sell": "trade_type",
                "Open price": "entry_price",
                "Close price": "exit_price",
                "Lots": "lot_size",
                "Profit": "pnl",
                "Net profit": "net_profit",
                "MAE": "mae",
                "MFE": "mfe",
                "Open time": "timestamp",
                "Pips": "pips",
                "T/P": "tp",
                "S/L": "sl",
                "Trade duration (hours)": "trade_duration_hours",
            },
            inplace=True,
        )

        # Timestamp cleanup
        if "timestamp" in df:
            df["timestamp"] = pd.to_datetime(
                df["timestamp"], errors="coerce"
            ).dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Required columns check
        required = [
            "timestamp",
            "symbol",
            "trade_type",
            "entry_price",
            "exit_price",
            "lot_size",
            "pnl",
        ]
        if not all(col in df.columns for col in required):
            logging.warning(f" → Skipping {blob.name}: missing one of {required}")
            return

        # Numeric coercion
        for col in [
            "entry_price",
            "exit_price",
            "pnl",
            "net_profit",
            "mae",
            "mfe",
            "pips",
            "tp",
            "sl",
            "trade_duration_hours",
        ]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # GPT placeholders
        for col in [
            "gpt_inferred_strategy",
            "gpt_strategy_confidence",
            "gpt_trade_evaluation",
            "gpt_alternative_action",
            "was_gpt_recommendation_followed",
            "gpt_impact_alignment",
        ]:
            df.setdefault(col, None)

        # Dedupe by hash
        if "Ticket" in df.columns:
            df["__hash"] = df.apply(
                lambda r: hashlib.md5(
                    f"{r['account_id']}{r['Ticket']}{r['timestamp']}".encode()
                ).hexdigest(),
                axis=1,
            )
            df.drop_duplicates(subset="__hash", inplace=True)
            df.drop(columns="__hash", inplace=True)

        # Upsert in batch
        conn = get_db_conn()
        cur = conn.cursor()
        vals = []
        for _, r in df.iterrows():
            vals.append(
                (
                    int(r["Ticket"]),
                    r["account_id"],
                    r["symbol"],
                    r["trade_type"],
                    r["entry_price"],
                    r["exit_price"],
                    r["timestamp"],
                    r["lot_size"],
                    r["pnl"],
                    r.get("net_profit"),
                    r.get("mae"),
                    r.get("mfe"),
                    r.get("pips"),
                    r.get("tp"),
                    r.get("sl"),
                    r.get("trade_duration_hours"),
                    r["gpt_inferred_strategy"],
                    r["gpt_strategy_confidence"],
                    r["gpt_trade_evaluation"],
                    r["gpt_alternative_action"],
                    r["was_gpt_recommendation_followed"],
                    r["gpt_impact_alignment"],
                )
            )
        cur.executemany(UPSERT_SQL, vals)
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Finished {blob.name}: {len(vals)} rows upserted")

    except Exception as e:
        logging.error(f"Error processing {blob.name}: {e}", exc_info=True)


# ─── Entry Point ────────────────────────────────────────────────────────────
def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=CSV_PREFIX))

    logging.info(f"Discovered {len(blobs)} CSV files under '{CSV_PREFIX}'")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_blob, b) for b in blobs]
        for f in as_completed(futures):
            pass  # all logging is inside process_blob()


if __name__ == "__main__":
    main()
