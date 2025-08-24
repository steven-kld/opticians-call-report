from datetime import datetime, timedelta, date
import json, re
import pandas as pd
import numpy as np
from db_utils import run_query, update_metrics_with_flags
from ai_utils import detect_voicemail, detect_proactive, detect_new_patient, detect_dropped, detect_booked

def day_bounds(d):
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end

def get_raw_on_date(d):
    start, end = day_bounds(d)
    raw = run_query(
        """
        SELECT
            t.call_id,
            t.duration_sec,
            t.call_time,
            t.transcript,
            t.phone_key,
            m.call_type,
            m.practice,
            m.is_answered,
            m.is_proactive,
            m.is_booked,
            m.is_new_patient,
            m.is_voicemail,
            m.is_dropped
        FROM transcriptions AS t
        JOIN metrics AS m USING (call_id)
        WHERE t.call_time >= %s AND t.call_time < %s
        ORDER BY t.call_time
        """,
        (start, end),
        fetch_all=True
    )
    return pd.DataFrame(raw)


def build_practice_report(raw_df):
    practices = sorted(raw_df['practice'].unique())
    
    # Helper to aggregate counts
    def agg_counts(df, conditions):
        counts = []
        for p in practices:
            sub = df[df['practice'] == p]

            if isinstance(conditions, str):
                # Check for specific counting conditions first
                if conditions == 'True':
                    counts.append(len(sub)) # Correctly count all rows
                # Check for complex boolean expressions
                elif any(op in conditions for op in ['&', '|', '~']):
                    counts.append(sub.eval(conditions).sum())
                else:
                    # Treat as a column name for simple strings
                    counts.append(sub[conditions].sum())
            else:
                # Handle non-string conditions (e.g., a list of columns)
                counts.append(sub[conditions].sum())

        counts.append(sum(counts))  # Total
        return counts
    
    def format_duration(seconds_list):
        formatted_list = []
        for s in seconds_list:
            if s is not None:
                # Calculate total minutes using integer division
                total_minutes = round(int(s) // 60, 1)
                formatted_list.append(total_minutes)
            else:
                formatted_list.append(None)  # Retain None for missing values
        return formatted_list

    report_rows = []

    # --- Outbound Calls ---
    outbound = raw_df[raw_df['call_type'] == 'outbound']
    report_rows.append(['Outbound Answered Directly'] + agg_counts(outbound, 'is_answered & ~is_dropped & ~is_voicemail'))
    report_rows.append(['Outbound Voicemails Received'] + agg_counts(outbound, 'is_voicemail'))
    report_rows.append(['Outbound Dropped (Unanswered)'] + agg_counts(outbound, 'is_dropped & ~is_voicemail'))
    report_rows.append(['Outbound Booked'] + agg_counts(outbound, 'is_booked'))
    report_rows.append(['Outbound Proactive Recalls'] + agg_counts(outbound, 'is_proactive'))
    report_rows.append(['Outbound Booked from Proactive Recall'] + agg_counts(outbound, 'is_booked & is_proactive'))
    report_rows.append(['Outbound Total Calls'] + agg_counts(outbound, 'True'))
    outbound_duration_secs = agg_counts(outbound, 'duration_sec')
    report_rows.append(['Outbound Duration min'] + format_duration(outbound_duration_secs))
    
    # --- Inbound Calls ---
    inbound = raw_df[raw_df['call_type'] == 'inbound']
    report_rows.append([''])
    report_rows.append(['Inbound Answered Directly'] + agg_counts(inbound, 'is_answered & ~is_dropped & ~is_voicemail'))
    report_rows.append(['Inbound Dropped (Unanswered)'] + agg_counts(inbound, 'is_dropped'))
    report_rows.append(['Inbound Booked'] + agg_counts(inbound, 'is_booked'))
    report_rows.append(['Inbound Total Calls'] + agg_counts(inbound, 'True'))
    inbound_duration_secs = agg_counts(inbound, 'duration_sec')
    report_rows.append(['Inbound Duration min'] + format_duration(inbound_duration_secs))


    # --- Total Calls ---
    report_rows.append([''])
    report_rows.append(['Total Calls'] + agg_counts(raw_df, 'is_answered'))
    report_rows.append(['Total Answered'] + agg_counts(raw_df, 'is_answered & ~is_dropped & ~is_voicemail'))
    report_rows.append(['Total Booked'] + agg_counts(raw_df, 'is_booked'))
    total_duration_secs = agg_counts(raw_df, 'duration_sec')
    report_rows.append(['Total Duration min'] + format_duration(total_duration_secs))

    columns = [''] + practices + ['Total']
    report_df = pd.DataFrame(report_rows, columns=columns)
    return report_df

# d = date(2025, 8, 14)
# raw = get_raw_on_date(d)
# report = build_practice_report(raw)
# report.to_csv("filename.csv")
