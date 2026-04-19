ALTER TABLE threads
    ADD COLUMN organization_id UUID,
    -- message_count remains at 0 for existing rows; new writes update counts.
    ADD COLUMN message_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_threads_organization_created_at ON threads (organization_id, created_at DESC, id DESC);
