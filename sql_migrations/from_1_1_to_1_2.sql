-- Создание таблицы журнала сообщений
CREATE TABLE IF NOT EXISTS api_messages_journal (
    id SERIAL PRIMARY KEY,
    message JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    action_id INTEGER REFERENCES actions(id)
);

-- Предоставление прав на новую таблицу
GRANT ALL PRIVILEGES ON TABLE api_messages_journal TO birthdaybot;
GRANT ALL PRIVILEGES ON SEQUENCE api_messages_journal_id_seq TO birthdaybot;
