from fastapi import FastAPI, UploadFile, File, HTTPException, Response
from fastapi.responses import HTMLResponse
import asyncio, re, zipfile, os, openai, tempfile, json
from pathlib import Path
from datetime import datetime
import pandas as pd
from io import BytesIO

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def upload_form():
    return """
<!doctype html>
<html>
  <body>
    <h3>1. Upload CSVs → get aggregated report</h3>
    <form id="csvForm">
      <input type="file" name="csv_files" accept=".csv" multiple required>
      <button type="submit">Build report</button>
    </form>
    <hr>
    <h3>2. Upload audio ZIP → get call-id,time,transcription</h3>
    <form id="zipForm">
      <input type="file" name="zip_file" accept=".zip" required>
      <button type="submit">Transcribe</button>
    </form>
    <hr>
    <h3>3. Match report + calls CSVs</h3>
    <form id="matchForm">
        <input type="file" name="csv_files" accept=".csv" multiple required>
        <button type="submit">Match</button>
    </form>
    <hr>

    <script>
      async function postAndDownload(formElem, url) {
        const data = new FormData(formElem);
        const res = await fetch(url, { method: 'POST', body: data });
        if (!res.ok) {
          alert(await res.text().catch(()=>res.statusText));
          return;
        }
        const blob = await res.blob();
        const cd = res.headers.get('Content-Disposition') || '';
        const m = cd.match(/filename="?(.*?)"?$/);
        const filename = (m && m[1]) || 'result.csv';
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(a.href);
      }
      document.getElementById('csvForm').addEventListener('submit', (e) => { e.preventDefault(); postAndDownload(e.target, '/upload_csv'); });
      document.getElementById('zipForm').addEventListener('submit', (e) => { e.preventDefault(); postAndDownload(e.target, '/upload_zip'); });
      document.getElementById('matchForm').addEventListener('submit', (e) => { e.preventDefault(); postAndDownload(e.target, '/match_csv'); });
    </script>
  </body>
</html>
    """

def extract_site(name: str):
    sites = ["Cheadle", "Heald Green", "Middleton", "Heckmondwike"]
    for s in sites:
        if s.lower().replace(" ", "") in name.lower().replace(" ", ""):
            return s
    return None

def extract_phone_key(name: str):
    m = re.search(r"-(\d+)_", name)
    if not m:
        return None
    num = m.group(1)
    if len(num) >= 7:
        return num[-7:]
    return num

def init_openai():
    return openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def extract_json_block(text: str):
    match = re.search(r"(\{.*\}|\[.*\])", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON block found in GPT response")

    block = match.group(0)
    block = re.sub(r"^```(?:json)?", "", block.strip())
    block = re.sub(r"```$", "", block.strip())

    return json.loads(block)

def extract_mmss_from_filename(name: str):
    if not isinstance(name, str):
        return pd.NA, pd.NA, pd.NA
    m = re.search(r"_(\d{14})", name)
    if not m:
        return pd.NA, pd.NA, pd.NA
    ts = m.group(1)  # YYYYMMDDHHMMSS
    try:
        mm = int(ts[10:12])
        ss = int(ts[12:14])
    except Exception:
        return pd.NA, pd.NA, pd.NA
    mmss_sec = mm * 60 + ss  # 0..3599
    return mm, ss, mmss_sec

def transcribe_one(raw: bytes):
    print("Transcribing one")
    openai_client = init_openai()
    with tempfile.NamedTemporaryFile(suffix=".wav") as tmp:
        tmp.write(raw)
        tmp.flush()

        with open(tmp.name, "rb") as f:
            response = openai_client.audio.transcriptions.create(
                model="gpt-4o-transcribe",
                file=f,
                response_format="text",
            )

        prompt = f"""
Take the following call transcript. It is a two-party phone conversation
between a manager (receptionist/staff) and a client (caller).
Rewrite it as a JSON array, turn by turn, where each item is an object
with a single key ("manager" or "client") and the corresponding utterance as value.

Transcript:
{response}
"""

        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a parser that converts transcripts into structured JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
        )

        structured_json = extract_json_block(completion.choices[0].message.content)

    return structured_json
        
@app.post("/upload_zip")
async def handle_zip_upload(zip_file: UploadFile = File(...)):
    if not zip_file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="ZIP must be .zip")

    # Stream upload to a temp file instead of reading into memory
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        while True:
            chunk = await zip_file.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            tmp.write(chunk)
        path = tmp.name
    await zip_file.close()

    try:
        wav_rows, tasks = [], []
        sem = asyncio.Semaphore(4)  # keep small on 1GB instances
        loop = asyncio.get_running_loop()

        def read_member(zf, info):
            with zf.open(info, "r") as f:
                return f.read()

        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                if info.is_dir() or not info.filename.lower().endswith(".wav"):
                    continue
                name = Path(info.filename).name

                async def _t(info=info, name=name):
                    async with sem:
                        raw = await loop.run_in_executor(None, read_member, zf, info)
                        return await loop.run_in_executor(None, transcribe_one, raw)

                tasks.append(_t())
                wav_rows.append({
                    "filename": name,
                    "site": extract_site(name),
                    "phone key": extract_phone_key(name),
                })

            transcripts = await asyncio.gather(*tasks, return_exceptions=True)

        for row, tr in zip(wav_rows, transcripts):
            if isinstance(tr, Exception):
                row["transcript"] = ""
                row["error"] = repr(tr)
            else:
                row["transcript"] = tr

        csv_bytes = pd.DataFrame(wav_rows).to_csv(index=False).encode("utf-8")
        fname = f'calls_{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.csv'
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'}
        )
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


@app.post("/upload_csv")
async def handle_upload(
    csv_files: list[UploadFile] = File(...),
):
    if not csv_files:
        raise HTTPException(status_code=400, detail="At least one CSV is required")

    print("Start processing report")

    dfs = []
    for f in csv_files:
        data = await f.read()
        await f.close()
        try:
            df = pd.read_csv(BytesIO(data), low_memory=False)
            dfs.append(df)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"{f.filename}: {e}")

    combined = pd.concat(dfs, ignore_index=True).drop_duplicates()

    REQUIRED = [
        "Call Time", "Call ID", "From", "Cost",
        "Direction", "Status", "Call Activity Details"
    ]
    missing = [c for c in REQUIRED if c not in combined.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing columns: {missing}. Got: {list(combined.columns)}"
        )

    combined["_ts"] = pd.to_datetime(combined["Call Time"], errors="coerce")
    combined = combined.sort_values(["Call ID", "_ts"], kind="mergesort")  # stable order

    def join_series(s: pd.Series) -> str:
        vals = [str(x) for x in s.dropna().astype(str)]
        return ", ".join(vals)

    agg = {
        "Call Time": "first",
        "From": "first",
        "Cost": "first",
        "Direction": join_series,
        "Status": join_series,
        "Call Activity Details": join_series,
    }

    per_call_df = combined.groupby("Call ID", as_index=False).agg(agg)
    per_call_df = per_call_df.drop(columns=["_ts"], errors="ignore")

    # serialize to CSV
    report_df = pd.DataFrame(per_call_df)
    print(report_df)

    csv_bytes = report_df.to_csv(index=False).encode("utf-8")
    fname = f'report_{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.csv'

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )

@app.post("/match_csv")
async def handle_upload(csv_files: list[UploadFile] = File(...)):
    if len(csv_files) != 2:
        raise HTTPException(status_code=400, detail="Need exactly 2 CSV files: report and calls")

    report_df, calls_df = None, None
    for f in csv_files:
        data = await f.read()
        await f.close()
        df = pd.read_csv(BytesIO(data), low_memory=False)
        name = (f.filename or "").lower()
        if "report" in name:
            report_df = df
        elif "calls" in name:
            calls_df = df
        else:
            raise HTTPException(status_code=400, detail=f"Filename must contain 'report' or 'calls': {f.filename}")

    if report_df is None or calls_df is None:
        raise HTTPException(status_code=400, detail="Both report and calls CSVs are required")

    # ---- Validate required columns
    if "phone key" not in calls_df.columns:
        raise HTTPException(status_code=400, detail="Calls CSV missing required column: 'phone key'")
    if "filename" not in calls_df.columns:
        raise HTTPException(status_code=400, detail="Calls CSV missing required column: 'filename'")
    if "Call Time" not in report_df.columns:
        raise HTTPException(status_code=400, detail="Report CSV missing required column: 'Call Time'")
    if report_df.shape[1] == 0:
        raise HTTPException(status_code=400, detail="Report CSV has no columns")

    # ---- Report prep
    rpt = report_df.copy()
    report_id_col = rpt.columns[0]  # first column is the ID to return
    rpt["_report_dt"] = pd.to_datetime(rpt["Call Time"], errors="coerce")
    rpt["_rep_mmss_sec"] = (rpt["_report_dt"].dt.minute * 60 + rpt["_report_dt"].dt.second).astype("Int64")
    # digits-only haystack to build ids candidates by phone key
    rpt["_hay_digits"] = (
        rpt.astype(str)
           .apply(lambda s: " | ".join(s.values), axis=1)
           .str.replace(r"\D", "", regex=True)
    )

    # ---- Calls prep
    calls = calls_df.copy()

    # phone key normalize (keep all digits), min len = 5
    MIN_LEN = 5
    calls["_pk"] = calls["phone key"].astype(str).str.replace(r"\D", "", regex=True)

    # candidate ids by phone key containment
    ids_json = []
    for _, call in calls.iterrows():
        pk = call["_pk"]
        if not pk or len(pk) < MIN_LEN:
            ids_json.append(json.dumps([], ensure_ascii=False))
            continue
        mask = rpt["_hay_digits"].str.contains(re.escape(pk), na=False)
        ids = rpt.loc[mask, report_id_col].astype(str).unique().tolist()
        ids_json.append(json.dumps(ids, ensure_ascii=False))
    calls["ids"] = ids_json

    # extract MM:SS and numeric key (0..3599) from filename
    calls[["call_min", "call_sec", "call_mmss_sec"]] = (
        calls["filename"]
            .apply(lambda s: pd.Series(extract_mmss_from_filename(s)))
            .astype("Int64")
    )
    calls["min-sec key"] = calls.apply(
        lambda r: f"{int(r['call_min']):02d}:{int(r['call_sec']):02d}"
        if pd.notna(r["call_min"]) and pd.notna(r["call_sec"]) else None,
        axis=1
    )

    # ---- Select closest report id by MM:SS among candidate ids (<= 30s circular)
    # Build id -> rep_mmss_sec map (as string ids for consistency with calls["ids"])
    id_map = (
        rpt[[report_id_col, "_rep_mmss_sec"]]
        .dropna()
        .assign(**{report_id_col: lambda d: d[report_id_col].astype(str)})
        .set_index(report_id_col)["_rep_mmss_sec"]
        .to_dict()
    )

    def pick_best_id(ids_json_str, call_mmss_sec):
        try:
            cand_ids = json.loads(ids_json_str) if isinstance(ids_json_str, str) else []
        except Exception:
            cand_ids = []
        if pd.isna(call_mmss_sec) or not cand_ids:
            return "[]", pd.NA

        c = int(call_mmss_sec)
        best_id, best_delta = None, None
        for rid in cand_ids:
            rep_sec = id_map.get(str(rid))
            if rep_sec is None or pd.isna(rep_sec):
                continue
            r = int(rep_sec)
            diff = abs(c - r)
            delta = min(diff, 3600 - diff)  # circular on 3600s
            if delta <= 60 and (best_delta is None or delta < best_delta):
                best_id, best_delta = str(rid), int(delta)

        if best_id is None:
            return "[]", pd.NA
        return json.dumps([best_id], ensure_ascii=False), best_delta

    best = calls.apply(
        lambda r: pd.Series(pick_best_id(r["ids"], r["call_mmss_sec"]), index=["ids_new", "ids_delta_sec"]),
        axis=1
    )
    calls["ids"] = best["ids_new"]
    calls["ids_delta_sec"] = best["ids_delta_sec"]

    # ---- Build per-report attachment from chosen calls
    def _first_id(ids_json_str):
        try:
            arr = json.loads(ids_json_str) if isinstance(ids_json_str, str) else []
        except Exception:
            arr = []
        return arr[0] if arr else None

    calls["__best_id"] = calls["ids"].apply(_first_id).astype("string")

    # choose transcript column (prefer 'transcript', else 'transcription_json', else empty)
    _transcript_col = "transcript" if "transcript" in calls.columns else ("transcription_json" if "transcription_json" in calls.columns else None)
    if _transcript_col is None:
        calls["__transcript_tmp"] = ""
        _transcript_col = "__transcript_tmp"

    # keep only rows with a chosen id; for collisions keep the smallest delta
    calls_attach = (
        calls.dropna(subset=["__best_id"])
            .assign(__best_id=lambda d: d["__best_id"].astype(str))
            .sort_values(["__best_id", "ids_delta_sec"], kind="mergesort")
            .drop_duplicates(subset=["__best_id"], keep="first")
            .rename(columns={"__best_id": report_id_col})
            [[report_id_col, "filename", "site", "phone key", _transcript_col, "ids_delta_sec"]]
            .rename(columns={_transcript_col: "transcript"})
    )

    # ---- Merge onto report and return report CSV
    report_out = report_df.copy()
    
    # ensure key types align
    report_out[report_id_col] = report_out[report_id_col].astype(str)
    calls_attach[report_id_col] = calls_attach[report_id_col].astype(str)

    report_out = report_out.merge(calls_attach, on=report_id_col, how="left")

    # --- Impute site for rows with missing filename (no call attached)
    known_sites = ["Cheadle", "Heald Green", "Middleton", "Heckmondwike"]

    if "site" not in report_out.columns:
        report_out["site"] = pd.NA

    idx = report_out.index

    # rows missing filename
    missing_file = (
        ~report_out["filename"].notna()
        if "filename" in report_out.columns
        else pd.Series(False, index=idx)
    )

    # rows with empty site
    site_empty = report_out["site"].isna() | report_out["site"].astype(str).str.strip().eq("")

    # search text
    text_series = ""
    if "From" in report_out.columns:
        text_series += report_out["From"].astype(str) + " "
    if "Call Activity Details" in report_out.columns:
        text_series += report_out["Call Activity Details"].astype(str)

    text_series = text_series.str.lower()

    mask_candidates = missing_file & site_empty

    # assign known sites
    for s in known_sites:
        s_lower = s.lower().replace(" ", "")
        site_mask = mask_candidates & text_series.str.replace(" ", "").str.contains(s_lower, na=False)
        report_out.loc[site_mask, "site"] = s
        mask_candidates = mask_candidates & ~site_mask  # remove assigned

    # assign Winsford if still empty
    winsford_mask = mask_candidates & text_series.str.contains("winsford", na=False)
    report_out.loc[winsford_mask, "site"] = "Winsford"

    csv_bytes = report_out.to_csv(index=False).encode("utf-8")
    fname = f'matched_report_{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.csv'
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )