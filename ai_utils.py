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


def detect_voicemail(transcript):
    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = f"""
Take the following call transcript. It is a two-party phone conversation
between an optical store manager (receptionist/staff) and a client.
Decide if an OUTBOUND call reached voicemail (answering machine / mailbox).

Rules:
- Voicemail: carrier/device mailbox prompts ("leave a message after the tone", "voicemail service", "unable to take your call"), then optional caller message.
- Not voicemail: human picked up (names, back-and-forth), or business IVR/queue/agent pickup (e.g., "you're through to Chris", "X speaking"), even if there was hold music or IVR before.
- If transcript is too garbled/insufficient, mark as not voicemail.

Respond with a single unmarked word TRUE if it's a voicemail, respond with FALSE if the call reached the client.

Transcript:
{transcript}
"""

    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Output only TRUE or FALSE."},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )

    resp = completion.choices[0].message.content.strip().lower()
    print(resp)
    print("-----------") 
    return bool(re.search(r"\btrue\b", resp))

def detect_proactive(transcript):
    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = f"""
You are a strict boolean classifier.

Task: Decide if the OUTBOUND call below is a PROACTIVE RECALL/REMINDER (TRUE) or NOT (FALSE).

Definitions:
- PROACTIVE (TRUE): Staff initiate the call to arrange a **new routine appointment** (recall, yearly/bi-yearly exam, follow-up monitoring). A new booking is discussed/created during this call.
- NOT PROACTIVE (FALSE): 
  - Calls confirming or reminding about an appointment that is already booked (e.g., “see you tomorrow,” “just reminding you for 2:45”).  
  - Calls where no new appointment is arranged.  
  - Calls about admin issues, glasses, orders, or returning missed calls.  
  - Too short/unclear transcripts.

Ambiguity rule:
- Only treat as TRUE if a new booking (recall) is clearly the purpose.

Output policy:
- Respond with a SINGLE WORD: TRUE or FALSE (no punctuation or extra text).

Transcript:
\"\"\"{transcript.strip()}\"\"\"
"""

    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Output only TRUE or FALSE."},
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )
    resp = completion.choices[0].message.content.strip().lower()
    print(transcript)
    print(resp)
    print("-----------")
    return bool(re.search(r"\btrue\b", resp))


def detect_new_patient(transcript):
    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = f"""
You are a strict boolean classifier. Output only TRUE or FALSE.

Task: Decide if this INBOUND call is a NEW PATIENT CALL (caller is not an existing patient).

Classify as NEW PATIENT (TRUE) if ANY of the following are evident:
- Caller or staff confirm: “I haven’t been before”, “I’m not registered”, “This is my first time.”
- Caller requests to register: “I’d like to register”, “I’ve just moved to the area.”
- Caller asks about services/pricing (e.g., glazing/reglazing, lens options, exam price) AND it’s clear they are not on record.
- Staff asks “Are you registered / Have you been to us before?” and the caller answers NO.
- Caller provides personal details for the first time (e.g., DOB, address, history) in the context of registering.

Classify as NOT NEW PATIENT (FALSE) if:
- Caller references an existing record or prior visits (recalls, rebooking, collections, existing orders, ongoing issues).
- Caller is vendor/other business, wrong number, or calling about someone already on file.

Ambiguity rule:
- If the transcript is too short/unclear to decide, output FALSE.

Output policy:
- Respond with a SINGLE WORD: TRUE or FALSE (no punctuation or extra text).

Transcript:
\"\"\"{transcript.strip()}\"\"\"
"""
    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Output only TRUE or FALSE."},
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )
    resp = completion.choices[0].message.content.strip().lower()
    # safer than substring match: require standalone 'true'
    print(transcript)
    print(resp)
    print("-----------")
    return bool(re.search(r"\btrue\b", resp))


def detect_dropped(transcript):
    if len(transcript) > 300: return False

    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = f"""
You are a strict boolean classifier. Output only TRUE or FALSE.

Task: Decide if this call was DROPPED/UNANSWERED.

Rules:
- FALSE (not dropped): There is a clear conversation between at least two people AND the call ends with a human closing signal such as “bye”, “thank you”, “see you”, “okay, we’ll see you tomorrow”.
- TRUE (dropped/unanswered): All other cases — including system/IVR messages, single-speaker greetings, calls ending abruptly without a closing, or conversations that do not finish with a polite ending.

Ambiguity rule:
- If unsure, lean TRUE.

Output policy:
- Respond with a SINGLE WORD: TRUE or FALSE.

Transcript:
{transcript}
"""

    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Output only TRUE or FALSE."},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )

    resp = completion.choices[0].message.content.strip().lower()
    print(transcript)
    print(resp)
    print("---------")
    if "true" in resp:
        return True
    else:
        return False
    

def detect_booked(transcript):
    openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = f"""
You are a strict boolean classifier. Output only TRUE or FALSE.

Task: Decide if this call LED TO A NEW BOOKING (a new appointment was created on this call).

Return TRUE only if ALL:
- A specific appointment slot (date and/or time) is selected during this call (e.g., “27th of August 9:45”, “Wednesday 1:45”, “tomorrow at 10”).
- The caller accepts that slot (e.g., “yes”, “that’s fine”, “I’ll take that”).
- Staff confirm they are booking or have booked it (e.g., “I’ll book that in”, “you’re booked”, “that’s confirmed”, “I’ll send a confirmation text/email”).
  NOTE: Mentioning a later confirmation call/text is still TRUE if booking is stated.

Return FALSE if any:
- It’s only confirming or verifying an existing booking (e.g., “see you tomorrow”, “linking your online booking”).
- It’s a reschedule/change of an existing appointment.
- Availability discussed but no final acceptance; or staff only “block/hold” without booking; or caller will “get back”.
- Reminder/recall without choosing and booking a slot.
- Voicemail/unanswered; or ambiguous/unclear outcome.

Output policy:
- Respond with a SINGLE WORD: TRUE or FALSE.

Transcript:
{transcript}
"""

    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Output only TRUE or FALSE."},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )

    resp = completion.choices[0].message.content.strip().lower()
    print(transcript)
    print(resp)
    print("---------")
    if "true" in resp:
        return True
    else:
        return False