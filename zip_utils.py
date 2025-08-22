import asyncio, json, os, re, shutil, tempfile, zipfile
from pathlib import Path
from fastapi import UploadFile
from typing import Dict, List
from datetime import datetime
import pandas as pd
from ai_utils import transcribe_one
from db_utils import query_all, run_query

def extract_site(name: str):
    sites = ["Cheadle", "Heald Green", "Middleton", "Heckmondwike", "Winsford"]
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

def extract_datetime_from_filename(name: str):
    if not isinstance(name, str):
        return pd.NA, pd.NA
    m = re.search(r"_(\d{14})", name)
    if not m:
        return pd.NA, pd.NA
    ts = m.group(1)  # 'YYYYMMDDHHMMSS'
    try:
        dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
        return dt, dt.isoformat(sep=" ")
    except Exception:
        return pd.NA, pd.NA


async def save_zip(zip_file: UploadFile):
    chunk_size = 1024 * 1024
    zip_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            zip_path = tmp.name
            while True:
                chunk = await zip_file.read(chunk_size)
                if not chunk:
                    break
                tmp.write(chunk)
        return zip_path
    finally:
        try:
            await zip_file.close()
        except Exception:
            pass

def get_wav_names_zip(zip_path):
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = [
                Path(i.filename).name
                for i in zf.infolist()
                if (not i.is_dir()) and i.filename.lower().endswith(".wav")
            ]
            # de-dupe, preserve order
            seen, unique = set(), []
            for n in names:
                if n not in seen:
                    seen.add(n)
                    unique.append(n)
            return unique or None
    except zipfile.BadZipFile:
        try: os.remove(zip_path)
        except Exception: pass
        return None
    
def get_existing_calls(filenames):
    if not filenames:
        return set()
    rows = query_all(
        "SELECT filename FROM transcriptions WHERE filename = ANY(%s)",
        (list(filenames),)
    )
    return {r["filename"] for r in rows}

def extract_selected_wavs(zip_path: str, selected: List[str]):
    tmpdir = tempfile.mkdtemp(prefix="unzipped_")
    name_to_path: Dict[str, str] = {}

    with zipfile.ZipFile(zip_path) as zf:
        # Put into a set for O(1) membership checks
        wanted = set(selected)
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = Path(info.filename).name
            if base in wanted:
                out_path = os.path.join(tmpdir, base)
                with zf.open(info) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)  # streamed copy
                name_to_path[base] = out_path

    return tmpdir, name_to_path

async def _transcription_worker(
    name_to_path: Dict[str, str],
    to_process: List[str],
    zip_path: str,
    tmpdir: str,
):
    loop = asyncio.get_running_loop()

    def _read_bytes(p: str) -> bytes:
        with open(p, "rb") as f:
            return f.read()

    try:
        for file_name in to_process:
            path = name_to_path.get(file_name)
            if not path:
                print(f"skip (no path): {file_name}")
                continue

            site = extract_site(file_name) or ""
            phone_key = extract_phone_key(file_name) or ""
            dt, iso = extract_datetime_from_filename(file_name)
            call_time = iso or None

            try:
                raw = await loop.run_in_executor(None, _read_bytes, path)
                tr = await loop.run_in_executor(None, transcribe_one, raw)
                transcript = json.dumps(tr, ensure_ascii=False)
                print(f"{file_name} | site={site} | phone_key={phone_key} | call_time={call_time}")
                print(transcript)
                run_query(
                    """
                    INSERT INTO transcriptions (filename, site, phone_key, transcript, call_time)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (filename) DO NOTHING
                    """,
                    (file_name, site, phone_key, transcript, call_time,)
                )
            except Exception as e:
                print(f"FAIL {file_name} | {e!r}")
    finally:
        # cleanup artifacts
        try: os.remove(zip_path)
        except Exception: pass
        try: shutil.rmtree(tmpdir)
        except Exception: pass

def schedule_transcription_job(
    name_to_path: Dict[str, str],
    to_process: List[str],
    zip_path: str,
    tmpdir: str,
):
    asyncio.create_task(_transcription_worker(name_to_path, to_process, zip_path, tmpdir))