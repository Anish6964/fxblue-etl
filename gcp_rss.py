import os
import logging
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import feedparser
import psycopg2
from datetime import datetime
import pytz
from google.cloud import storage

# ─── Configuration via ENV VARs ─────────────────────────────────────────────
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

BUCKET_NAME = os.environ["BUCKET_NAME"]
ACCOUNTS_FILE = os.environ.get("ACCOUNTS_FILE", "rss_data/30_RSS_Accounts.xlsx")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 20))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))  # trades per batch upsert

# ─── Logging Setup ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── DB Utilities ───────────────────────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


META_UPSERT = """
INSERT INTO account_metadata
  (account_id, account_url, rss_url, trade_win, total_return, trades_per_day,
   strategy_inferred, gpt_comments)
VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL)
ON CONFLICT(account_id) DO UPDATE SET
  account_url      = EXCLUDED.account_url,
  rss_url          = EXCLUDED.rss_url,
  trade_win        = EXCLUDED.trade_win,
  total_return     = EXCLUDED.total_return,
  trades_per_day   = EXCLUDED.trades_per_day;
"""

TRADES_UPSERT = """
INSERT INTO rss_trades (
  account_id, account_url, rss_url, trade_win, total_return, trades_per_day,
  account_balance, account_equity, account_floating_profit, account_closed_profit,
  account_free_margin, ticket, action, lots, symbol,
  open_price, close_price, open_time, close_time,
  profit, swap, commission, total_profit, take_profit,
  stop_loss, magic_number,
  gpt_recommendation_issued,
  gpt_recommendation_content,
  gpt_recommendation_accuracy,
  gpt_suggestion_score,
  trade_deviation_reasoning
)
VALUES (%s)
ON CONFLICT(ticket) DO UPDATE SET
  account_id                 = EXCLUDED.account_id,
  account_url                = EXCLUDED.account_url,
  rss_url                    = EXCLUDED.rss_url,
  trade_win                  = EXCLUDED.trade_win,
  total_return               = EXCLUDED.total_return,
  trades_per_day             = EXCLUDED.trades_per_day,
  account_balance            = EXCLUDED.account_balance,
  account_equity             = EXCLUDED.account_equity,
  account_floating_profit    = EXCLUDED.account_floating_profit,
  account_closed_profit      = EXCLUDED.account_closed_profit,
  account_free_margin        = EXCLUDED.account_free_margin,
  action                     = EXCLUDED.action,
  lots                       = EXCLUDED.lots,
  symbol                     = EXCLUDED.symbol,
  open_price                 = EXCLUDED.open_price,
  close_price                = EXCLUDED.close_price,
  open_time                  = EXCLUDED.open_time,
  close_time                 = EXCLUDED.close_time,
  profit                     = EXCLUDED.profit,
  swap                       = EXCLUDED.swap,
  commission                 = EXCLUDED.commission,
  total_profit               = EXCLUDED.total_profit,
  take_profit                = EXCLUDED.take_profit,
  stop_loss                  = EXCLUDED.stop_loss,
  magic_number               = EXCLUDED.magic_number,
  
"""

# gpt_recommendation_issued      = EXCLUDED.gpt_recommendation_issued,
# gpt_recommendation_content     = EXCLUDED.gpt_recommendation_content,
# gpt_recommendation_accuracy    = EXCLUDED.gpt_recommendation_accuracy,
# gpt_suggestion_score           = EXCLUDED.gpt_suggestion_score,
# trade_deviation_reasoning      = EXCLUDED.trade_deviation_reasoning;


# ─── Timestamp Normalization ────────────────────────────────────────────────
def normalize_timestamp(ts_str: str):
    """Parse 'Thu 21 Mar 2019 09:00:11' → UTC ISO8601 string, or None."""
    try:
        dt = datetime.strptime(ts_str, "%a %d %b %Y %H:%M:%S")
        return pytz.utc.localize(dt).isoformat()
    except Exception:
        return None


# ─── Per-Account Processing ─────────────────────────────────────────────────
def process_account(row):
    sa = row["username"]
    url = row["account_url"]
    rss = row["rss_url"]

    # parse percent fields
    def to_pct(x):
        if pd.isna(x) or x == "-":
            return None
        return float(x.strip("%")) / 100 if isinstance(x, str) else float(x)

    win = to_pct(row.get("trade win"))
    ret = to_pct(row.get("Total return"))
    tpd = to_pct(row.get("Trades per day"))

    try:
        # upsert metadata
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(META_UPSERT, (sa, url, rss, win, ret, tpd))

        # fetch feed
        feed = feedparser.parse(rss)
        trades = []
        # placeholders
        bal = eq = fp = cp = fm = None

        for item in feed.entries:
            # account summary
            if hasattr(item, "account_balance"):
                bal = float(item.account_balance)
                eq = float(item.account_equity)
                fp = float(item.account_floatingprofit)
                cp = float(item.account_closedprofit)
                fm = float(item.account_freemargin)

            # positions only
            if not hasattr(item, "position_ticket"):
                continue

            ticket = int(item.position_ticket)
            action = item.position_action
            lots = float(item.position_lots) if item.position_lots else None
            symbol = item.position_symbol
            op = float(item.position_openprice) if item.position_openprice else None
            cl = float(item.position_closeprice) if item.position_closeprice else None
            ot = normalize_timestamp(item.position_opentime)
            ct = (
                normalize_timestamp(item.position_closetime)
                if item.position_closetime != "Thu 1 Jan 1970 00:00:00"
                else None
            )
            profit = float(item.position_profit) if item.position_profit else None
            swap = float(item.position_swap) if item.position_swap else None
            comm = float(item.position_commission) if item.position_commission else None
            tpft = (
                float(item.position_totalprofit) if item.position_totalprofit else None
            )
            tp_ = float(item.position_tp) if item.position_tp != "0" else None
            sl_ = float(item.position_sl) if item.position_sl != "0" else None
            mag = int(item.position_magicnumber) if item.position_magicnumber else None

            trades.append(
                (
                    sa,
                    url,
                    rss,
                    win,
                    ret,
                    tpd,
                    bal,
                    eq,
                    fp,
                    cp,
                    fm,
                    ticket,
                    action,
                    lots,
                    symbol,
                    op,
                    cl,
                    ot,
                    ct,
                    profit,
                    swap,
                    comm,
                    tpft,
                    tp_,
                    sl_,
                    mag,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            )

            # batch upserts
            if len(trades) >= BATCH_SIZE:
                cur.executemany(TRADES_UPSERT.replace("(%s)", "(%s)" * 31), trades)
                trades.clear()

        # final batch
        if trades:
            cur.executemany(TRADES_UPSERT.replace("(%s)", "(%s)" * 31), trades)

        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"✅ {sa}: metadata+{len(trades)} trades upserted")

    except Exception as e:
        logging.error(f"❌ Error for {sa}: {e}", exc_info=True)


# ─── Entry Point ────────────────────────────────────────────────────────────
def main():
    # download the account list Excel
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(ACCOUNTS_FILE)
    data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data))

    logging.info(f"Loaded {len(df)} accounts from {ACCOUNTS_FILE}")

    # parallelize
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_account, row) for _, row in df.iterrows()]
        for _ in as_completed(futures):
            pass  # each call logs its own success/failure


if __name__ == "__main__":
    main()
