CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE threads (
    id UUID PRIMARY KEY,
    status SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE thread_participants (
    thread_id UUID NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    participant_id UUID NOT NULL,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, participant_id)
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    thread_id UUID NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    sender_id UUID NOT NULL,
    body TEXT NOT NULL,
    file_ids TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE message_recipients (
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    thread_id UUID NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    participant_id UUID NOT NULL,
    acked_at TIMESTAMPTZ,
    PRIMARY KEY (message_id, participant_id)
);

CREATE INDEX idx_thread_participants_participant ON thread_participants (participant_id, thread_id);
CREATE INDEX idx_messages_thread_created_at ON messages (thread_id, created_at, id);
CREATE INDEX idx_message_recipients_participant_ack ON message_recipients (participant_id, acked_at, message_id);
CREATE INDEX idx_message_recipients_thread_participant ON message_recipients (thread_id, participant_id);
