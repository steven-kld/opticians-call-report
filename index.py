from fastapi import FastAPI, UploadFile, Form, File, HTTPException, Response
from fastapi.responses import HTMLResponse, StreamingResponse
import re, os, json
from datetime import datetime
import pandas as pd
from io import BytesIO
from db_utils import insert_raw_report_df
from zip_utils import save_zip, get_wav_names_zip, get_existing_calls, extract_selected_wavs, schedule_transcription_job
from join_utils import join_calls_at_date
from transcript_utils import generate_flags_from_transcripts
from report_utils import get_raw_on_date, build_practice_report

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

    <h3>2. Upload audio ZIP → count files to process</h3>
    <form id="zipForm">
      <input type="file" name="zip_file" accept=".zip" required>
      <button type="submit">Check</button>
    </form>
    <pre id="zipOut" style="white-space:pre-wrap;"></pre>
    <hr>

    <h3>3. Generate report by date</h3>
    <form id="dateForm">
      <input type="date" name="report_date" required>
      <button type="submit">Get XLSX</button>
    </form>

    <script>
      async function postAndDownload(formElem, url) {
        const data = new FormData(formElem);
        const res = await fetch(url, { method: 'POST', body: data });
        if (!res.ok) {
          alert(await res.text().catch(() => res.statusText));
          return;
        }
        const blob = await res.blob();
        const cd = res.headers.get('Content-Disposition') || '';
        const m = cd.match(/filename="?(.*?)"?$/);
        const filename = (m && m[1]) || 'result.xlsx';
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(a.href);
      }

      document.getElementById('csvForm')
        .addEventListener('submit', (e) => { e.preventDefault(); postAndDownload(e.target, '/upload_csv'); });

      document.getElementById('zipForm')
        .addEventListener('submit', async (e) => {
          e.preventDefault();
          const out = document.getElementById('zipOut');
          out.textContent = 'Processing…';

          const data = new FormData(e.target);
          const res = await fetch('/upload_zip', { method: 'POST', body: data });

          if (!res.ok) {
            out.textContent = await res.text().catch(() => res.statusText);
            return;
          }

          const ct = res.headers.get('Content-Type') || '';
          if (ct.includes('application/json')) {
            const payload = await res.json();
            const { to_process_count = 0 } = payload || {};
            out.textContent = `Files to process: ${to_process_count}`;
          } else {
            await postAndDownload(e.target, '/upload_zip');
          }
        });

      // New date form
      document.getElementById('dateForm')
        .addEventListener('submit', (e) => { 
          e.preventDefault(); 
          postAndDownload(e.target, '/report_by_date'); 
        });
    </script>
  </body>
</html>
    """

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

    insert_raw_report_df(report_df)
    
    csv_bytes = report_df.to_csv(index=False).encode("utf-8")
    fname = f'report_{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.csv'

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )

@app.post("/upload_zip")
async def upload_zip(zip_file: UploadFile = File(...)):
    if not zip_file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="ZIP must be .zip")

    zip_path = await save_zip(zip_file)
    if not zip_path:
        return {"to_process_count": 0}
    
    try:
        wav_names = get_wav_names_zip(zip_path)
        if not wav_names:
            try: os.remove(zip_path)
            except Exception: pass
            return {"to_process_count": 0}

        existing = get_existing_calls(wav_names)
        to_process = [n for n in wav_names if n not in existing]

        if not to_process:
            try: os.remove(zip_path)
            except Exception: pass
            return {"to_process_count": 0}
        
        tmpdir, name_to_path = extract_selected_wavs(zip_path, to_process)

        schedule_transcription_job(name_to_path, to_process, zip_path, tmpdir)

        return {"to_process_count": len(to_process)}
    
    except Exception:
        try: os.remove(zip_path)
        except Exception: pass
        return {"to_process_count": 0}

@app.post("/report_by_date")
async def report_by_date(report_date: str = Form(...)):
    # Convert date string to datetime.date
    dt = datetime.strptime(report_date, "%Y-%m-%d").date()

    raw_df = get_raw_on_date(dt)
    report_df = build_practice_report(raw_df)
    
    # Write to in-memory XLSX with two sheets
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="report")
        raw_df.to_excel(writer, index=False, sheet_name="raw data")
    output.seek(0)

    filename = f"calls-{dt}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# TODO
# @app.post("/manual_process")
# async def manual_process(date):
#     d = date(2025, 8, 14)
#     join_calls_at_date(d)
#     generate_flags_from_transcripts(d)
