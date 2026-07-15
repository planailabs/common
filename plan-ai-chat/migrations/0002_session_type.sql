-- One unified session store for all agent-chat domains, separated by a
-- session_type discriminator ('chat' = fleet chatbot, 'healer' = healer, ...).
-- Existing rows predate the column and are all fleet-chatbot sessions.

ALTER TABLE chat_sessions
    ADD COLUMN IF NOT EXISTS session_type TEXT NOT NULL DEFAULT 'chat';

CREATE INDEX IF NOT EXISTS idx_chat_sessions_type
    ON chat_sessions (session_type);
