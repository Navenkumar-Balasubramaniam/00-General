# warehouse_checks.py
import os
import sys
import pymysql
from datetime import datetime, timezone, timedelta

DB_HOST = os.getenv("MARIADB_HOST", "localhost")
DB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
DB_USER = os.getenv("MARIADB_USER", "ecom")
DB_PASS = os.getenv("MARIADB_PASS", "ecom123")
DB_NAME = os.getenv("MARIADB_DB", "ecom_warehouse")

# SLI thresholds (tune these)
MIN_USERS = int(os.getenv("SLI_MIN_USERS", "1"))
MIN_EVENTS = int(os.getenv("SLI_MIN_EVENTS", "1"))
MIN_ORDERS = int(os.getenv("SLI_MIN_ORDERS", "1"))
FRESHNESS_HOURS = int(os.getenv("SLI_FRESHNESS_HOURS", "24"))

def q1(cur, sql, args=None):
    cur.execute(sql, args or ())
    return cur.fetchone()

def qall(cur, sql, args=None):
    cur.execute(sql, args or ())
    return cur.fetchall()

def fail(msg):
    print(f"❌ FAIL: {msg}")
    return False

def ok(msg):
    print(f"✅ OK: {msg}")
    return True

def main():
    all_ok = True

    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

    with conn.cursor() as cur:
        # 1) Row counts (basic SLI)
        users = q1(cur, "SELECT COUNT(*) AS c FROM dim_user")["c"]
        events = q1(cur, "SELECT COUNT(*) AS c FROM fact_event")["c"]
        orders = q1(cur, "SELECT COUNT(*) AS c FROM fact_order")["c"]

        all_ok &= ok(f"dim_user count = {users}") if users >= MIN_USERS else fail(f"dim_user count {users} < {MIN_USERS}")
        all_ok &= ok(f"fact_event count = {events}") if events >= MIN_EVENTS else fail(f"fact_event count {events} < {MIN_EVENTS}")
        all_ok &= ok(f"fact_order count = {orders}") if orders >= MIN_ORDERS else fail(f"fact_order count {orders} < {MIN_ORDERS}")

        # 2) PK uniqueness / nulls
        pk_checks = [
            ("dim_user", "user_id"),
            ("fact_event", "event_id"),
            ("fact_order", "order_id"),
        ]
        for table, pk in pk_checks:
            nulls = q1(cur, f"SELECT COUNT(*) AS c FROM {table} WHERE {pk} IS NULL")["c"]
            dups = q1(cur, f"""
                SELECT COUNT(*) AS c FROM (
                    SELECT {pk}
                    FROM {table}
                    GROUP BY {pk}
                    HAVING COUNT(*) > 1
                ) t
            """)["c"]
            all_ok &= ok(f"{table}.{pk} has no NULLs") if nulls == 0 else fail(f"{table}.{pk} NULLs = {nulls}")
            all_ok &= ok(f"{table}.{pk} has no duplicates") if dups == 0 else fail(f"{table}.{pk} duplicate keys = {dups}")

        # 3) FK integrity for user_id
        missing_users_event = q1(cur, """
            SELECT COUNT(*) AS c
            FROM fact_event fe
            LEFT JOIN dim_user du ON fe.user_id = du.user_id
            WHERE fe.user_id IS NOT NULL AND du.user_id IS NULL
        """)["c"]

        missing_users_order = q1(cur, """
            SELECT COUNT(*) AS c
            FROM fact_order fo
            LEFT JOIN dim_user du ON fo.user_id = du.user_id
            WHERE fo.user_id IS NOT NULL AND du.user_id IS NULL
        """)["c"]

        all_ok &= ok("fact_event.user_id all exist in dim_user") if missing_users_event == 0 else fail(f"fact_event has {missing_users_event} user_id orphans")
        all_ok &= ok("fact_order.user_id all exist in dim_user") if missing_users_order == 0 else fail(f"fact_order has {missing_users_order} user_id orphans")

        # 4) Freshness SLI (max ingested_at)
        # (If ingested_at is NULL everywhere, this will show as NULL)
        max_event_ingest = q1(cur, "SELECT MAX(ingested_at) AS mx FROM fact_event")["mx"]
        max_order_ingest = q1(cur, "SELECT MAX(ingested_at) AS mx FROM fact_order")["mx"]

        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)

        def freshness_check(label, dt):
            nonlocal all_ok
            if dt is None:
                all_ok &= fail(f"{label} MAX(ingested_at) is NULL")
                return
            # MariaDB returns naive datetime; assume local/server time -> compare as naive
            # (If you store UTC, better: convert properly later)
            age_ok = dt >= cutoff.replace(tzinfo=None)
            all_ok &= ok(f"{label} freshness OK (max ingested_at={dt})") if age_ok else fail(f"{label} stale (max ingested_at={dt}, cutoff={cutoff})")

        freshness_check("fact_event", max_event_ingest)
        freshness_check("fact_order", max_order_ingest)

        # 5) Optional: orphan orders by event_id (report only)
        orphan_orders = q1(cur, """
            SELECT COUNT(*) AS c
            FROM fact_order o
            LEFT JOIN fact_event e ON o.event_id = e.event_id
            WHERE o.event_id IS NOT NULL AND e.event_id IS NULL
        """)["c"]
        print(f"ℹ️  INFO: orphan fact_order.event_id (no matching fact_event) = {orphan_orders} (not failing)")

    conn.close()

    if not all_ok:
        print("❌ Warehouse checks failed.")
        sys.exit(1)

    print("✅ All warehouse checks passed.")
    sys.exit(0)

if __name__ == "__main__":
    main()
