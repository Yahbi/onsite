-- Marketplace core tables for Onsite
-- Safe to run on SQLite or Postgres (adjust types as needed)

CREATE TABLE IF NOT EXISTS contractors (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    full_name TEXT,
    company TEXT,
    license_number TEXT,
    license_state TEXT,
    license_status TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS territories (
    zip_code TEXT PRIMARY KEY,
    median_home_value NUMERIC DEFAULT 0,
    permit_volume_score NUMERIC DEFAULT 0,
    desirability_index NUMERIC DEFAULT 1,
    base_price NUMERIC DEFAULT 249
);

CREATE TABLE IF NOT EXISTS territory_seats (
    id TEXT PRIMARY KEY,
    zip_code TEXT REFERENCES territories(zip_code),
    plan_type TEXT,
    seat_limit INTEGER,
    active_count INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS territory_seats_plan_zip_idx
ON territory_seats(zip_code, plan_type);

CREATE TABLE IF NOT EXISTS territory_waitlist (
    id TEXT PRIMARY KEY,
    zip_code TEXT REFERENCES territories(zip_code),
    contractor_id TEXT REFERENCES contractors(id),
    position INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS territory_pricing (
    id TEXT PRIMARY KEY,
    zip_code TEXT REFERENCES territories(zip_code),
    plan_type TEXT,
    price NUMERIC,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    contractor_id TEXT REFERENCES contractors(id),
    zip_code TEXT REFERENCES territories(zip_code),
    plan_type TEXT,
    status TEXT DEFAULT 'active',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ends_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lead_views (
    id TEXT PRIMARY KEY,
    contractor_id TEXT REFERENCES contractors(id),
    lead_id TEXT,
    viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS competitor_activity (
    id TEXT PRIMARY KEY,
    zip_code TEXT,
    activity_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS upsell_features (
    id TEXT PRIMARY KEY,
    contractor_id TEXT REFERENCES contractors(id),
    feature_code TEXT,
    status TEXT DEFAULT 'active',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Access audit log
CREATE TABLE IF NOT EXISTS access_audit (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    endpoint TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip TEXT,
    user_agent TEXT
);

-- Contractor license snapshots
CREATE TABLE IF NOT EXISTS contractor_license (
    user_id TEXT,
    license_number TEXT,
    status TEXT,
    classification TEXT,
    expiration DATE,
    last_verified_at TIMESTAMP,
    PRIMARY KEY(user_id)
);

-- Territory assignments (entitlements)
CREATE TABLE IF NOT EXISTS territory_assignments (
    id TEXT PRIMARY KEY,
    user_id TEXT REFERENCES contractors(id),
    zip_code TEXT REFERENCES territories(zip_code),
    plan_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Permit requests storage
CREATE TABLE IF NOT EXISTS permit_requests (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    lead_id TEXT,
    city TEXT,
    payload_json TEXT,
    pdf_url TEXT,
    status TEXT DEFAULT 'submitted',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts delivery log (idempotency + tracking)
CREATE TABLE IF NOT EXISTS alerts_delivery_log (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    channel TEXT,
    message_hash TEXT,
    delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT,
    UNIQUE(user_id, channel, message_hash)
);

-- Dead letter queue for lead failures
CREATE TABLE IF NOT EXISTS failed_leads_queue (
    id TEXT PRIMARY KEY,
    payload_json TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retries INTEGER DEFAULT 0
);

-- Behavioral logging tables for passive outcome capture
CREATE TABLE IF NOT EXISTS contractor_actions (
    id TEXT PRIMARY KEY,
    contractor_id TEXT,
    property_id INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    action_type TEXT,
    action_metadata TEXT
);
CREATE INDEX IF NOT EXISTS contractor_actions_idx ON contractor_actions(contractor_id, property_id, timestamp);

CREATE TABLE IF NOT EXISTS homeowner_responses (
    id TEXT PRIMARY KEY,
    contractor_id TEXT,
    property_id INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    response_type TEXT
);
CREATE INDEX IF NOT EXISTS homeowner_responses_idx ON homeowner_responses(contractor_id, property_id, timestamp);

CREATE TABLE IF NOT EXISTS project_outcomes (
    id TEXT PRIMARY KEY,
    property_id INTEGER,
    contractor_id TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    outcome TEXT,
    contract_value NUMERIC,
    project_type TEXT
);
-- allow multiple unknowns but only one won/lost per property+contractor
CREATE UNIQUE INDEX IF NOT EXISTS project_outcomes_winlose_idx
ON project_outcomes(property_id, contractor_id, outcome)
WHERE outcome IN ('won','lost');

CREATE TABLE IF NOT EXISTS contractor_insights (
    id TEXT PRIMARY KEY,
    contractor_id TEXT,
    month TEXT,
    summary_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS contractor_insights_unique ON contractor_insights(contractor_id, month);

CREATE TABLE IF NOT EXISTS prompt_throttle (
    contractor_id TEXT,
    property_id INTEGER,
    last_prompt_at TIMESTAMP,
    session_id TEXT,
    PRIMARY KEY(contractor_id, property_id)
);
-- Patch 2026-02-20: waitlist + cancellation tracking
CREATE TABLE IF NOT EXISTS cancellation_events (
    id TEXT PRIMARY KEY,
    contractor_id TEXT,
    zip TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    recovery_stage INTEGER DEFAULT 0
);
