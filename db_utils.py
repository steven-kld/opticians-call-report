import os
import psycopg2
from psycopg2.errors import UniqueViolation, ForeignKeyViolation
from psycopg2.extras import execute_values, RealDictCursor
import pandas as pd

# from dotenv import load_dotenv
# load_dotenv()

def get_db_config():
    return {
        "host": os.getenv("POSTGRESQL_HOST"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USER"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "port": os.getenv("POSTGRESQL_PORT", 5432),
        "sslmode": "require",
        "options": f"endpoint={os.getenv('POSTGRESQL_ENDPOINT')}"
    }

def get_conn():
    return psycopg2.connect(**get_db_config())

def insert_raw_report_df(df: pd.DataFrame):
    COLUMNS = ['Call ID','Call Time','From','Cost','Direction','Status','Call Activity Details']
    REQUIRED = ['Call ID','Call Time','From','Direction','Status']  # NOT NULLs in your table

    df = df[COLUMNS].copy()

    # Normalize/trim strings (avoids '  id  ' and empty -> None)
    for col in ['Call ID','From','Direction','Status','Call Activity Details']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    df.replace({'': None}, inplace=True)

    # Types
    dt = pd.to_datetime(df['Call Time'], errors='coerce')
    df['Call Time'] = [d.to_pydatetime() if pd.notna(d) else None for d in dt]
    df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce')  # stays as float (matches FLOAT)

    # Enforce NOT NULLs by dropping bad rows
    bad = df[REQUIRED].isnull().any(axis=1)
    if bad.any():
        df = df[~bad]

    if df.empty:
        return 0

    rows = list(df.itertuples(index=False, name=None))

    sql = """
        INSERT INTO raw_report
          (call_id, call_time, call_from, call_cost, call_direction, call_status, call_activity_details)
        VALUES %s
        ON CONFLICT (call_id) DO NOTHING
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)

def run_query(query, params=None, fetch_one=False, fetch_all=False):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())

                if fetch_one:
                    row = cur.fetchone()
                    if not row:
                        return None
                    cols = [desc[0] for desc in cur.description]
                    return dict(zip(cols, row))

                if fetch_all:
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, r)) for r in rows]

                conn.commit()
                return None
    except UniqueViolation:
        raise ValueError(f"Already exists")
    except ForeignKeyViolation:
        raise ValueError(f"Invalid foreign key")
    except Exception as e:
        raise RuntimeError(f"Failed to insert: {str(e)}")


def query_all(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]

