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
            m.call_id,
            r.phone_key,
            r.call_duration AS duration_sec,
            m.call_time,
            t.transcript,
            m.call_type,
            m.practice,
            m.is_answered,
            m.is_proactive,
            m.is_booked,
            m.is_new_patient,
            m.is_voicemail,
            m.is_dropped,
            r.is_redirected,
            r.is_recalled,
            r.recall_id
        FROM metrics AS m
        LEFT JOIN transcriptions AS t USING (call_id)
        LEFT JOIN raw_report AS r USING (call_id)
        WHERE m.call_time >= %s AND m.call_time < %s
        ORDER BY m.call_time
        """,
        (start, end),
        fetch_all=True
    )
    return pd.DataFrame(raw)


# def build_practice_report(raw_df):
#     practices = sorted(raw_df['practice'].unique())
    
#     # Helper to aggregate counts
#     def agg_counts(df, conditions):
#         counts = []
#         for p in practices:
#             sub = df[df['practice'] == p]

#             if isinstance(conditions, str):
#                 # Check for specific counting conditions first
#                 if conditions == 'True':
#                     counts.append(len(sub)) # Correctly count all rows
#                 # Check for complex boolean expressions
#                 elif any(op in conditions for op in ['&', '|', '~']):
#                     counts.append(sub.eval(conditions).sum())
#                 else:
#                     # Treat as a column name for simple strings
#                     counts.append(sub[conditions].sum())
#             else:
#                 # Handle non-string conditions (e.g., a list of columns)
#                 counts.append(sub[conditions].sum())

#         counts.append(sum(counts))  # Total
#         return counts
    
#     def format_duration(seconds_list):
#         formatted_list = []
#         for s in seconds_list:
#             if s is not None:
#                 # Calculate total minutes using integer division
#                 total_minutes = round(int(s) // 60, 1)
#                 formatted_list.append(total_minutes)
#             else:
#                 formatted_list.append(None)  # Retain None for missing values
#         return formatted_list

#     report_rows = []

#     # --- Outbound Calls ---
#     outbound = raw_df[raw_df['call_type'] == 'outbound']
#     report_rows.append(['Outbound Answered Directly'] + agg_counts(outbound, 'is_answered & ~is_dropped & ~is_voicemail'))
#     report_rows.append(['Outbound Voicemails Received'] + agg_counts(outbound, 'is_voicemail'))
#     report_rows.append(['Outbound Dropped (Unanswered)'] + agg_counts(outbound, 'is_dropped & ~is_voicemail'))
#     report_rows.append(['Outbound Booked'] + agg_counts(outbound, 'is_booked'))
#     report_rows.append(['Outbound Proactive Recalls'] + agg_counts(outbound, 'is_proactive'))
#     report_rows.append(['Outbound Booked from Proactive Recall'] + agg_counts(outbound, 'is_booked & is_proactive'))
#     report_rows.append(['Outbound Total Calls'] + agg_counts(outbound, 'True'))
#     outbound_duration_secs = agg_counts(outbound, 'duration_sec')
#     report_rows.append(['Outbound Duration min'] + format_duration(outbound_duration_secs))
    
#     # --- Inbound Calls ---
#     inbound = raw_df[raw_df['call_type'] == 'inbound']
#     report_rows.append([''])
#     report_rows.append(['Inbound Answered Directly'] + agg_counts(inbound, 'is_answered & ~is_dropped & ~is_voicemail'))
#     report_rows.append(['Inbound Dropped (Unanswered)'] + agg_counts(inbound, 'is_dropped'))
#     report_rows.append(['Inbound Booked'] + agg_counts(inbound, 'is_booked'))
#     report_rows.append(['Inbound Total Calls'] + agg_counts(inbound, 'True'))
#     inbound_duration_secs = agg_counts(inbound, 'duration_sec')
#     report_rows.append(['Inbound Duration min'] + format_duration(inbound_duration_secs))


#     # --- Total Calls ---
#     report_rows.append([''])
#     report_rows.append(['Total Calls'] + agg_counts(raw_df, 'is_answered'))
#     report_rows.append(['Total Answered'] + agg_counts(raw_df, 'is_answered & ~is_dropped & ~is_voicemail'))
#     report_rows.append(['Total Booked'] + agg_counts(raw_df, 'is_booked'))
#     total_duration_secs = agg_counts(raw_df, 'duration_sec')
#     report_rows.append(['Total Duration min'] + format_duration(total_duration_secs))

#     columns = [''] + practices + ['Total']
#     report_df = pd.DataFrame(report_rows, columns=columns)
#     return report_df

def build_practice_report(raw_df):
    practices = sorted(raw_df['practice'].unique())
    
    # Helper to aggregate counts
    def agg_counts(df, conditions):
        counts = []
        for p in practices:
            sub = df[df['practice'] == p]
            if isinstance(conditions, str):
                if conditions == 'True':
                    counts.append(len(sub))
                elif any(op in conditions for op in ['&', '|', '~']):
                    counts.append(sub.eval(conditions).sum())
                else:
                    counts.append(sub[conditions].sum())
            else:
                counts.append(sub[conditions].sum())
        counts.append(sum(counts))
        return counts

    # Helper to aggregate durations (in seconds)
    def agg_duration(df):
        durations = []
        for p in practices:
            sub = df[df['practice'] == p]
            durations.append(sub['duration_sec'].sum())
        durations.append(sum(durations))
        return durations

    # Helper to format seconds to min:sec string
    def format_min_sec(seconds_list):
        formatted_list = []
        for s in seconds_list:
            if s is not None:
                s = int(s)
                minutes = s // 60
                seconds = s % 60
                formatted_list.append(f"{minutes}:{seconds:02d}")
            else:
                formatted_list.append('')
        return formatted_list

    # Helper for percentage calculations
    def agg_percentage(df, numerator_cond, denominator_cond):
        percentages = []
        for p in practices:
            sub = df[df['practice'] == p]
            numerator = sub.eval(numerator_cond).sum()
            
            if denominator_cond == 'True':
                denominator = len(sub)
            else:
                denominator = sub.eval(denominator_cond).sum()
            
            if denominator > 0:
                percentages.append(f"{round((numerator / denominator) * 100)}%")
            else:
                percentages.append("0%")
        
        # Calculate total percentage
        total_numerator = df.eval(numerator_cond).sum()
        if denominator_cond == 'True':
            total_denominator = len(df)
        else:
            total_denominator = df.eval(denominator_cond).sum()
            
        if total_denominator > 0:
            percentages.append(f"{round((total_numerator / total_denominator) * 100)}%")
        else:
            percentages.append("0%")
            
        return percentages

    report_rows = []
    
    # Filter DataFrames for Inbound and Outbound
    inbound = raw_df[raw_df['call_type'] == 'inbound']
    outbound = raw_df[raw_df['call_type'] == 'outbound']

    report_rows.append(['Total Calls'] + agg_counts(raw_df, 'True'))
    report_rows.append(['Duration (total)'] + format_min_sec(agg_duration(raw_df)))

    report_rows.append(['']) # Spacer row
    report_rows.append(['Inbound Calls'] + agg_counts(inbound, 'True'))
    report_rows.append(['Duration (inbound)'] + format_min_sec(agg_duration(inbound)))
    report_rows.append(['Redirected (inbound)'] + agg_counts(inbound, 'is_redirected'))
    report_rows.append(['Answered Directly (inbound)'] + agg_counts(inbound, 'is_answered & ~is_dropped & ~is_voicemail'))
    report_rows.append(['Voicemails Received (inbound)'] + agg_counts(inbound, 'is_voicemail'))
    report_rows.append(['Dropped/Unanswered (inbound)'] + agg_counts(inbound, 'is_dropped & ~is_voicemail'))
    report_rows.append(['Dropped & Voicemail Recalled (inbound)'] + agg_counts(inbound, 'is_dropped & is_recalled'))
    report_rows.append(['% of Calls Recalled (inbound)'] + agg_percentage(inbound, 'is_recalled', 'is_dropped'))
    report_rows.append(['Booked (from inbound recorded)'] + agg_counts(inbound, 'is_booked'))

    report_rows.append(['']) # Spacer row
    report_rows.append(['Outbound Calls'] + agg_counts(outbound, 'True'))
    report_rows.append(['Duration (outbound)'] + format_min_sec(agg_duration(outbound)))
    report_rows.append(['Dropped/Unanswered (outbound)'] + agg_counts(outbound, 'is_dropped'))

    report_rows.append(['']) # Spacer row
    recorded_outbound = outbound[outbound['transcript'].notna()]
    report_rows.append(['Outbound Recorded Calls'] + agg_counts(recorded_outbound, 'True'))
    report_rows.append(['Answered Calls (outbound recorded)'] + agg_counts(recorded_outbound, '~is_dropped & ~is_voicemail'))
    report_rows.append(['Voicemail (outbound recorded)'] + agg_counts(recorded_outbound, 'is_voicemail'))
    report_rows.append(['Dropped/Unanswered (outbound recorded)'] + agg_counts(recorded_outbound, 'is_dropped & ~is_voicemail'))
    report_rows.append(['Proactive Recalls (outbound recorded)'] + agg_counts(recorded_outbound, 'is_proactive'))
    report_rows.append(['Booked from Proactive (outbound recorded)'] + agg_counts(recorded_outbound, 'is_booked & is_proactive'))
    report_rows.append(['Conversion Rate Proactive % (outbound recorded)'] + agg_percentage(recorded_outbound, 'is_booked & is_proactive', 'is_proactive'))
    report_rows.append(['New Patient Calls (outbound recorded)'] + agg_counts(recorded_outbound, 'is_new_patient'))
    
    columns = [''] + practices + ['Total']
    report_df = pd.DataFrame(report_rows, columns=columns)
    return report_df
# d = date(2025, 8, 14)
# raw = get_raw_on_date(d)
# report = build_practice_report(raw)
# report.to_csv("filename.csv")
