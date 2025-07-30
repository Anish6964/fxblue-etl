import feedparser
import pandas as pd
from datetime import datetime
import pytz
import psycopg2

# 1) Load list of accounts + RSS URLs + metadata
df = pd.read_excel("30_RSS_Accounts.xlsx")


def normalize_timestamp(ts_str):
    """Parse 'Thu 21 Mar 2019 09:00:11' → UTC ISO8601."""
    try:
        dt = datetime.strptime(ts_str, "%a %d %b %Y %H:%M:%S")
        return pytz.utc.localize(dt).isoformat()
    except Exception:
        return None


# 2) DB connection params (Timescale + Postgres container)
conn = psycopg2.connect(
    host="localhost", port=5432, dbname="postgres", user="postgres", password="mini"
)
cur = conn.cursor()

# 3) Loop accounts, upsert metadata, fetch & upsert trades
for _, row in df.iterrows():
    acct_id = row["username"]
    acct_url = row["account_url"]
    rss_url = row["rss_url"]

    # handle '-' as NULL
    def to_pct(x):
        if pd.isna(x) or x == "-":
            return None
        if isinstance(x, str) and "%" in x:
            return float(x.strip("%")) / 100
        return float(x)

    win = to_pct(row["trade win"])
    ret = to_pct(row["Total return"])
    tpd = to_pct(row["Trades per day"])

    # --- upsert metadata ---
    cur.execute(
        """
        INSERT INTO account_metadata
          (account_id, account_url, rss_url, trade_win, total_return, trades_per_day,
           strategy_inferred, gpt_comments)
        VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL)
        ON CONFLICT(account_id) DO UPDATE
          SET account_url    = EXCLUDED.account_url,
              rss_url        = EXCLUDED.rss_url,
              trade_win      = EXCLUDED.trade_win,
              total_return   = EXCLUDED.total_return,
              trades_per_day = EXCLUDED.trades_per_day;
    """,
        (acct_id, acct_url, rss_url, win, ret, tpd),
    )

    # --- fetch & upsert trades ---
    feed = feedparser.parse(rss_url)

    # account‐level placeholders
    bal = eq = fp = cp = fm = None

    for item in feed.entries:
        # first capture the live account summary, if any
        if "account_balance" in item:
            bal = float(item.account_balance)
            eq = float(item.account_equity)
            fp = float(item.account_floatingprofit)
            cp = float(item.account_closedprofit)
            fm = float(item.account_freemargin)

        # then each open/closed position
        if "position_ticket" not in item:
            continue

        ticket = int(item.position_ticket)
        action = item.position_action
        lots = float(item.position_lots) if item.position_lots else None
        symbol = item.position_symbol
        op = float(item.position_openprice) if item.position_openprice else None
        clp = float(item.position_closeprice) if item.position_closeprice else None
        ot = normalize_timestamp(item.position_opentime)
        ct = (
            normalize_timestamp(item.position_closetime)
            if item.position_closetime != "Thu 1 Jan 1970 00:00:00"
            else None
        )
        profit = float(item.position_profit) if item.position_profit else None
        swap = float(item.position_swap) if item.position_swap else None
        comm = float(item.position_commission) if item.position_commission else None
        tpft = float(item.position_totalprofit) if item.position_totalprofit else None
        tp_ = float(item.position_tp) if item.position_tp != "0" else None
        sl_ = float(item.position_sl) if item.position_sl != "0" else None
        mag = int(item.position_magicnumber) if item.position_magicnumber else None

        # now upsert into rss_trades (31 cols = existing 26 + 5 new GPT fields)
        cur.execute(
            f"""
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
            ) VALUES ({','.join(['%s']*31)})
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
              
        """,
            (
                acct_id,
                acct_url,
                rss_url,
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
                clp,
                ot,
                ct,
                profit,
                swap,
                comm,
                tpft,
                tp_,
                sl_,
                mag,
                # five new GPT fields, all NULL for now:
                None,
                None,
                None,
                None,
                None,
            ),
        )

    # gpt_recommendation_issued  = EXCLUDED.gpt_recommendation_issued,
    #       gpt_recommendation_content = EXCLUDED.gpt_recommendation_content,
    #       gpt_recommendation_accuracy= EXCLUDED.gpt_recommendation_accuracy,
    #      gpt_suggestion_score       = EXCLUDED.gpt_suggestion_score,
    #     trade_deviation_reasoning  = EXCLUDED.trade_deviation_reasoning;

# 4) finalize
conn.commit()
cur.close()
conn.close()

print("✅ Done.  metadata rows + all trades upserted.")
