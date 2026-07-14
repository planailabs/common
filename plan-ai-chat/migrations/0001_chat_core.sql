-- Generic chat sessions, messages, and token spend events.
-- Column names match plan_ai_chat::store::pg::PgTables::chat().

CREATE TABLE IF NOT EXISTS chat_sessions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_id        UUID NOT NULL,
    subject         TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'created',
    state_data      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by      TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    error_message   TEXT,
    initial_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider        TEXT,
    model           TEXT,
    label           TEXT,
    token_budget    BIGINT NOT NULL DEFAULT 0,
    tokens_used     BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_scope
    ON chat_sessions (scope_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_subject
    ON chat_sessions (subject);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_state
    ON chat_sessions (state);

CREATE TABLE IF NOT EXISTS chat_messages (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id  UUID NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role        TEXT NOT NULL,
    content     TEXT NOT NULL,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session
    ON chat_messages (session_id, created_at ASC);

CREATE TABLE IF NOT EXISTS chat_token_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id    UUID NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    provider      TEXT NOT NULL,
    model         TEXT NOT NULL,
    input_tokens  INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_token_events_session
    ON chat_token_events (session_id);

CREATE INDEX IF NOT EXISTS idx_chat_token_events_created
    ON chat_token_events (created_at);
