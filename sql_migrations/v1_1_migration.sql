-- Создание таблицы администраторов
CREATE TABLE IF NOT EXISTS admins (
    id SERIAL PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL UNIQUE
);

-- Предоставление прав на новую таблицу
GRANT ALL PRIVILEGES ON TABLE admins TO birthdaybot;
GRANT ALL PRIVILEGES ON SEQUENCE admins_id_seq TO birthdaybot;