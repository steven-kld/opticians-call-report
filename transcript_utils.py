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

def get_transcriptions_on_date(d):
    start, end = day_bounds(d)
    return run_query(
        """
        SELECT
        t.*,
        m.call_type
        FROM transcriptions AS t
        JOIN metrics AS m
        USING (call_id)
        WHERE m.call_time >= %s AND m.call_time < %s
        ORDER BY call_time
        """,
        (start, end),
        fetch_all=True
    )

def generate_flags_from_transcripts(d):
    tr = pd.DataFrame(get_transcriptions_on_date(d))
    outbound = tr.loc[tr["call_type"] == "outbound", ["call_id", "call_type", "transcript"]].copy()
    inbound  = tr.loc[tr["call_type"] == "inbound",  ["call_id", "call_type", "transcript"]].copy()
    outbound["is_new_patient"] = False
    inbound["is_voicemail"] = False
    inbound["is_proactive"] = False

    # find voicemails
    outbound["is_voicemail"] = outbound["transcript"].apply(
        lambda di: detect_voicemail(di["raw"])
    )

    # only run detect_proactive where voicemail == False
    mask = outbound["is_voicemail"] == False
    outbound["is_proactive"] = False
    outbound.loc[mask, "is_proactive"] = outbound.loc[mask, "transcript"].apply(
        lambda di: detect_proactive(di["raw"])
    )

    # find new patients
    inbound["is_new_patient"] = inbound["transcript"].apply(
        lambda di: detect_new_patient(di["raw"])
    )


    calls = pd.concat([outbound, inbound], ignore_index=True)

    # find dropped calls
    calls["is_dropped"] = calls["transcript"].apply(
        lambda di: detect_dropped(di["raw"])
    )

    # find calls leading to booking from not dropped and not voicemail
    calls["is_booked"] = False
    eligible = (~calls["is_voicemail"]) & (~calls["is_dropped"])
    calls.loc[eligible, "is_booked"] = calls.loc[eligible, "transcript"].apply(
        lambda di: detect_booked(di["raw"])
    )

    update_metrics_with_flags(calls)

# d = date(2025, 8, 14)
# generate_flags_from_transcripts(d)