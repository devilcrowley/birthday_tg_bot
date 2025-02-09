-- Поддерживается только релиз 1.0.2.4 STABLE, на версиях выше использовать init_db.sql
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

-- Устанавливаем схему public по умолчанию
SET search_path TO public;

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
    FOREIGN KEY (team_member_id) REFERENCES team_members(id)
);

-- Создаем функцию для проверки логики
CREATE OR REPLACE FUNCTION check_request_member()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.type = 'request' THEN
        -- Проверяем, чтобы team_member_id не совпадал с исполнителем задачи
        IF EXISTS (
            SELECT 1 
            FROM year_tasks 
            WHERE id = NEW.task_id AND team_member_id = NEW.team_member_id
        ) THEN
            RAISE EXCEPTION 'team_member_id cannot be the same as the task owner for type=request';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создаем триггер для проверки перед вставкой
CREATE TRIGGER trigger_check_request_member
BEFORE INSERT OR UPDATE ON actions
FOR EACH ROW
EXECUTE FUNCTION check_request_member();

--Doublecheck по правам на таблицы (опционально)
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO birthdaybot;
--GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO birthdaybot;
