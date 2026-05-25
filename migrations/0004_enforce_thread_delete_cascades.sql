ALTER TABLE thread_participants
    DROP CONSTRAINT IF EXISTS thread_participants_thread_id_fkey,
    ADD CONSTRAINT thread_participants_thread_id_fkey
    FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE;

ALTER TABLE messages
    DROP CONSTRAINT IF EXISTS messages_thread_id_fkey,
    ADD CONSTRAINT messages_thread_id_fkey
    FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE;

ALTER TABLE message_recipients
    DROP CONSTRAINT IF EXISTS message_recipients_message_id_fkey,
    DROP CONSTRAINT IF EXISTS message_recipients_thread_id_fkey,
    ADD CONSTRAINT message_recipients_message_id_fkey
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    ADD CONSTRAINT message_recipients_thread_id_fkey
    FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE;
