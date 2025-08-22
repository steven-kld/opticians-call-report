import json, openai, os, re, tempfile

def extract_json_block(text: str):
    match = re.search(r"(\{.*\}|\[.*\])", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON block found in GPT response")

    block = match.group(0)
    block = re.sub(r"^```(?:json)?", "", block.strip())
    block = re.sub(r"```$", "", block.strip())

    return json.loads(block)

def transcribe_one(raw: bytes):
    print("Transcribing one")
    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    with tempfile.NamedTemporaryFile(suffix=".wav") as tmp:
        tmp.write(raw)
        tmp.flush()

        with open(tmp.name, "rb") as f:
            response = openai_client.audio.transcriptions.create(
                model="gpt-4o-transcribe",
                file=f,
                response_format="text",
            )

    return {"raw": response}