from datetime import datetime, timedelta, date
import re
import pandas as pd
import numpy as np
from db_utils import run_query, update_transcriptions_with_matches, insert_metrics_core

def day_bounds(d):
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end

def get_raw_report_on_date(d):
    start, end = day_bounds(d)
    return run_query(
        """
        SELECT *
        FROM raw_report
        WHERE call_time >= %s AND call_time < %s
        ORDER BY call_time
        """,
        (start, end),
        fetch_all=True
    )

def get_transcriptions_on_date(d):
    start, end = day_bounds(d)
    return run_query(
        """
        SELECT *
        FROM transcriptions
        WHERE call_time >= %s AND call_time < %s
        ORDER BY call_time
        """,
        (start, end),
        fetch_all=True
    )

def get_joined_on_date(d):
    start, end = day_bounds(d)
    return run_query(
        """
        SELECT
            r.*,
            t.filename,
            t.site,
            t.phone_key,
            t.transcript,
            t.duration_sec
        FROM raw_report r
        LEFT JOIN transcriptions t
        ON r.call_id = t.call_id
        WHERE r.call_time >= %s AND r.call_time < %s
        ORDER BY r.call_time;
        """,
        (start, end),
        fetch_all=True
    )

def fetch_dataframes_for_date(d):
    raw_rows  = get_raw_report_on_date(d) or []
    tran_rows = get_transcriptions_on_date(d) or []
    raw_df = pd.DataFrame(raw_rows)
    tran_df = pd.DataFrame(tran_rows)
    return raw_df, tran_df

def _digits_only(x) -> str:
    return re.sub(r"\D", "", "" if x is None else str(x))

def _ensure_hay_digits(raw_df: pd.DataFrame) -> pd.DataFrame:
    if "_hay_digits" in raw_df.columns:
        return raw_df
    out = raw_df.copy()
    out["_hay_digits"] = (
        (out.get("call_from", "").astype(str) + " | " + out.get("call_activity_details", "").astype(str))
        .str.replace(r"\D", "", regex=True)
    )
    return out

def candidates_by_phone_key(transcription_row, raw_df: pd.DataFrame) -> pd.DataFrame:
    pk_val = transcription_row.get("phone_key") if isinstance(transcription_row, dict) else transcription_row.get("phone_key", None)
    if pk_val is None and (isinstance(transcription_row, dict) or isinstance(transcription_row, pd.Series)):
        pk_val = transcription_row.get("phone key") if isinstance(transcription_row, dict) else transcription_row.get("phone key", None)

    pk = _digits_only(pk_val)
    if not pk or len(pk) < 6:
        return raw_df.iloc[0:0].copy()  # empty DF

    rdf = _ensure_hay_digits(raw_df)
    mask = rdf["_hay_digits"].str.contains(re.escape(pk), na=False)
    return rdf.loc[mask].copy()

def _mmss_sec(ts):
    if pd.isna(ts):
        return np.nan
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return np.nan
    return int(t.minute) * 60 + int(t.second)

def pick_best_by_mmss(row_time, cand_df: pd.DataFrame, time_col="call_time"):
    if cand_df.empty:
        return None, None

    c_mmss = cand_df[time_col].apply(_mmss_sec).astype("Int64")
    r_mmss = _mmss_sec(row_time)
    if pd.isna(r_mmss):
        return None, None

    diff = (c_mmss - int(r_mmss)).abs()
    delta = np.minimum(diff, 3600 - diff)  # circular
    # mask invalids
    valid = delta.notna()
    if not valid.any():
        return None, None

    best_pos = delta[valid].astype(int).idxmin()
    best_delta = int(delta.loc[best_pos])
    if best_delta > 60:
        return None, None
    return best_pos, best_delta

def match_all_calls(raw_df: pd.DataFrame, tran_df: pd.DataFrame) -> pd.DataFrame:
    rdf = _ensure_hay_digits(raw_df).copy()
    rdf["_mmss"] = rdf["call_time"].apply(_mmss_sec).astype("Int64")

    results = []
    for _, row in tran_df.iterrows():
        cand_df = candidates_by_phone_key(row, rdf)
        best_idx, best_delta = pick_best_by_mmss(row["call_time"], cand_df, time_col="call_time")
        if best_idx is None:
            results.append({
                "transcription_id": row.get("filename"),
                "raw_report_id": None,
                "delta_sec": None,
                "raw_call_time": None
            })
        else:
            best_row = cand_df.loc[best_idx]
            results.append({
                "transcription_id": row.get("filename"),
                "raw_report_id": best_row.get("call_id"),  # your actual ID col
                "delta_sec": best_delta,
                "raw_call_time": best_row.get("call_time")
            })

    out = pd.DataFrame(results)

    dup_mask = out["raw_report_id"].notna()
    if dup_mask.any():
        keep = (
            out[dup_mask]
            .sort_values(["raw_report_id", "delta_sec"], kind="mergesort")
            .drop_duplicates(subset=["raw_report_id"], keep="first")
            .assign(_keep=True)[["transcription_id", "_keep"]]
        )
        out = out.merge(keep, on="transcription_id", how="left")
        out.loc[out["_keep"].isna() & out["raw_report_id"].notna(), ["raw_report_id", "delta_sec", "raw_call_time"]] = [None, None, None]
        out = out.drop(columns="_keep")

    return out

def build_core_metrics(joined: pd.DataFrame) -> pd.DataFrame:
    metrics = pd.DataFrame()
    metrics["call_id"] = joined["call_id"]
    metrics["time_sec"] = joined["duration_sec"]
    # detecting call type
    metrics["call_type"] = None
    dir_lower = joined["call_direction"].astype(str).str.lower()
    metrics.loc[dir_lower.str.contains("inbound", na=False), "call_type"] = "inbound"
    metrics.loc[dir_lower.str.contains("outbound", na=False), "call_type"] = "outbound"
    metrics.loc[dir_lower.str.contains("internal", na=False), "call_type"] = "internal"
    
    # detecting answered/unanswered
    status = joined["call_status"].astype(str).str.lower()
    has_unanswered = status.str.contains(r"\bunanswered\b", regex=True, na=False)
    has_answered   = status.str.contains(r"\banswered\b",   regex=True, na=False)
    cond_unanswered_only = has_unanswered & ~has_answered
    metrics["is_answered"] = ~cond_unanswered_only
    
    # detecting practice
    metrics["practice"] = None
    sites = ["Cheadle", "Heald Green", "Middleton", "Heckmondwike", "Winsford"]
    haystack = (
        joined["call_from"].astype(str) + " " +
        joined["call_activity_details"].astype(str) + " " +
        joined["filename"].astype(str)
    ).str.lower()
    for site in sites:
        site_norm = site.lower().replace(" ", "")
        mask = haystack.str.replace(" ", "").str.contains(site_norm, na=False)
        metrics.loc[mask, "practice"] = site

    return metrics


# d = date(2025, 8, 14)

# raw_df, tran_df = fetch_dataframes_for_date(d)
# matches = match_all_calls(raw_df, tran_df)
# update_transcriptions_with_matches(matches)

# joined = pd.DataFrame(get_joined_on_date(d))
# core_metrics = build_core_metrics(joined)
# print(core_metrics)
# insert_metrics_core(core_metrics)
