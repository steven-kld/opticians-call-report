# install flyctl


# DB

``` sql
CREATE TABLE transcriptions (
    filename TEXT PRIMARY KEY,
    site VARCHAR(25) NOT NULL,
    phone_key  BIGINT NOT NULL,
    transcript JSONB NOT NULL,
    call_time TIMESTAMP NOT NULL,
    duration_sec INT,
    call_type  VARCHAR(25),
    score FLOAT,
    comment TEXT,
    call_id  VARCHAR(255)
);

CREATE TABLE raw_report (
    call_id VARCHAR(255) PRIMARY KEY,
    call_time TIMESTAMP NOT NULL,
    call_from VARCHAR(255) NOT NULL,
    call_cost FLOAT,
    call_direction VARCHAR(255) NOT NULL,
    call_status VARCHAR(255) NOT NULL,
    call_activity_details TEXT NOT NULL
);

CREATE TABLE metrics (
    call_id VARCHAR(255) PRIMARY KEY,
    call_time TIMESTAMP NOT NULL,
    call_type VARCHAR(25),
    status VARCHAR(25),
    practice VARCHAR(25),
    duration_sec INT,
    is_dropped BOOLEAN,
    is_proactive BOOLEAN,
    is_booked BOOLEAN,
    is_new_patient BOOLEAN,
    is_voicemail BOOLEAN,
    is_redirected BOOLEAN,
    is_answered BOOLEAN
);
```