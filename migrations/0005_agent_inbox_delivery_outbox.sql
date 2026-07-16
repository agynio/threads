CREATE TABLE agent_inbox_deliveries (
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    agent_instance_id UUID NOT NULL,
    thread_id UUID NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    sender_id UUID NOT NULL,
    body TEXT NOT NULL,
    file_ids TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, agent_instance_id)
);

CREATE INDEX idx_agent_inbox_deliveries_pending
    ON agent_inbox_deliveries (created_at, message_id, agent_instance_id)
    WHERE delivered_at IS NULL;
