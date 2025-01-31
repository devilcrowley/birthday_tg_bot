-- Создаем базу данных, если она не существует
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'birthdaybot') THEN
        CREATE DATABASE birthdaybot;
    END IF;
END
$$;

-- Подключаемся к базе данных birthdaybot
\c birthdaybot;

-- Проверяем существование схемы public
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'public') THEN
        CREATE SCHEMA public;
    END IF;
END
$$;

-- Устанавливаем схему public по умолчанию
SET search_path TO public;

-- Предоставляем базовые права на схему
GRANT USAGE ON SCHEMA public TO birthdaybot;
GRANT CREATE ON SCHEMA public TO birthdaybot;

-- Устанавливаем права по умолчанию для будущих объектов
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO birthdaybot;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO birthdaybot;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO birthdaybot;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT USAGE ON TYPES TO birthdaybot;

-- Создаем enum тип для actions
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action_type') THEN
        CREATE TYPE action_type AS ENUM ('request', 'payout');
    END IF;
END
$$;

-- Создаем таблицы
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS team_members (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    birthday DATE NOT NULL,
    team_id INTEGER NOT NULL,
    phone_number VARCHAR(50) NOT NULL,
    telegram_chat_id BIGINT,
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS teamleads (
    id SERIAL PRIMARY KEY,
    team_member_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    phone_number VARCHAR(50) NOT NULL,
    FOREIGN KEY (team_member_id) REFERENCES team_members(id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS year_tasks (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    team_member_id INTEGER NOT NULL,
    is_members_notified BOOLEAN DEFAULT false,
    is_teamlead_notified BOOLEAN DEFAULT false,
    is_money_transfered BOOLEAN DEFAULT false,
    FOREIGN KEY (team_member_id) REFERENCES team_members(id)
);

CREATE TABLE IF NOT EXISTS actions (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    team_member_id INTEGER NOT NULL,
    type action_type NOT NULL,
    is_done BOOLEAN DEFAULT false,
    FOREIGN KEY (task_id) REFERENCES year_tasks(id),
    FOREIGN KEY (team_member_id) REFERENCES team_members(id),
    CONSTRAINT check_request_member CHECK (
        type != 'request' OR 
        team_member_id NOT IN (
            SELECT team_member_id FROM year_tasks WHERE id = task_id
        )
    )
);

CREATE TABLE IF NOT EXISTS admins (
    id SERIAL PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL UNIQUE
);

-- Предоставляем права на созданные объекты
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO birthdaybot;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO birthdaybot;