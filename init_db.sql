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

-- Создание таблицы администраторов (v1.1 compatible minimum)
CREATE TABLE IF NOT EXISTS admins (
    id SERIAL PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL UNIQUE
);

-- Предоставление прав на новую таблицу
GRANT ALL PRIVILEGES ON TABLE admins TO birthdaybot;
GRANT ALL PRIVILEGES ON SEQUENCE admins_id_seq TO birthdaybot;

-- Создание таблицы журнала сообщений (v1.2 compatible minimum)
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

-- Функция для получения альтернативного тимлида (v1.4 compatible minimum)
CREATE OR REPLACE FUNCTION get_alternative_teamlead(team_id INTEGER, birthday_member_id INTEGER)
RETURNS TABLE (
    teamlead_id INTEGER,
    phone_number VARCHAR(50),
    member_name VARCHAR(100)
) AS $$
BEGIN
    -- Сначала пытаемся найти другого тимлида из той же команды
    RETURN QUERY
    SELECT 
        tl.id,
        tl.phone_number,
        tm.name
    FROM teamleads tl
    JOIN team_members tm ON tl.team_member_id = tm.id
    WHERE tl.team_id = $1  -- используем первый параметр функции (team_id)
    AND tl.team_member_id != $2  -- используем второй параметр (birthday_member_id)
    LIMIT 1;

    -- Если не нашли, возвращаем любого другого тимлида
    IF NOT FOUND THEN
        RETURN QUERY
        SELECT 
            tl.id,
            tl.phone_number,
            tm.name
        FROM teamleads tl
        JOIN team_members tm ON tl.team_member_id = tm.id
        WHERE tl.team_member_id != $2  -- используем второй параметр (birthday_member_id)
        LIMIT 1;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Обновляем запрос для уведомлений тимлида (v1.4 compatible minimum)
CREATE OR REPLACE VIEW teamlead_notifications AS
WITH birthday_info AS (
    SELECT 
        yt.id as task_id,
        bm.name as birthday_person_name,
        t.id as team_id,
        bm.id as birthday_member_id,
        -- Проверяем, является ли именинник тимлидом
        EXISTS (
            SELECT 1 
            FROM teamleads tl 
            WHERE tl.team_member_id = bm.id
        ) as is_birthday_person_teamlead
    FROM year_tasks yt
    JOIN team_members bm ON yt.team_member_id = bm.id
    JOIN teams t ON bm.team_id = t.id
    WHERE yt.is_teamlead_notified = false
),
teamlead_info AS (
    SELECT 
        bi.*,
        CASE 
            WHEN bi.is_birthday_person_teamlead THEN
                (SELECT member_name FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id))
            ELSE
                tlm.name
        END as notified_teamlead_name,
        CASE 
            WHEN bi.is_birthday_person_teamlead THEN
                (SELECT tm.telegram_chat_id 
                 FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id) alt
                 JOIN teamleads tl ON alt.teamlead_id = tl.id
                 JOIN team_members tm ON tl.team_member_id = tm.id)
            ELSE
                tlm.telegram_chat_id
        END as telegram_chat_id
    FROM birthday_info bi
    LEFT JOIN teamleads tl ON bi.team_id = tl.team_id
    LEFT JOIN team_members tlm ON tl.team_member_id = tlm.id
    WHERE NOT bi.is_birthday_person_teamlead 
    OR EXISTS (SELECT 1 FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id))
)
SELECT 
    task_id,
    birthday_person_name,
    telegram_chat_id,
    notified_teamlead_name
FROM teamlead_info;

-- Обновляем запрос для уведомлений участников (v1.4 compatible minimum)
CREATE OR REPLACE VIEW member_notifications AS
WITH birthday_info AS (
    SELECT 
        yt.id as task_id,
        bm.name as birthday_person_name,
        t.id as team_id,
        t.name as team_name,
        bm.id as birthday_member_id,
        -- Проверяем, является ли именинник тимлидом
        EXISTS (
            SELECT 1 
            FROM teamleads tl 
            WHERE tl.team_member_id = bm.id
        ) as is_birthday_person_teamlead
    FROM year_tasks yt
    JOIN team_members bm ON yt.team_member_id = bm.id
    JOIN teams t ON bm.team_id = t.id
    WHERE yt.is_members_notified = false
),
teamlead_info AS (
    SELECT 
        bi.*,
        CASE 
            WHEN bi.is_birthday_person_teamlead THEN
                (SELECT phone_number FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id))
            ELSE
                tl.phone_number
        END as teamlead_phone,
        CASE 
            WHEN bi.is_birthday_person_teamlead THEN
                (SELECT member_name FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id))
            ELSE
                tlm.name
        END as teamlead_name
    FROM birthday_info bi
    LEFT JOIN teamleads tl ON bi.team_id = tl.team_id
    LEFT JOIN team_members tlm ON tl.team_member_id = tlm.id
    WHERE NOT bi.is_birthday_person_teamlead 
    OR EXISTS (SELECT 1 FROM get_alternative_teamlead(bi.team_id, bi.birthday_member_id))
)
SELECT 
    a.id as action_id,
    ti.*,
    m.telegram_chat_id
FROM teamlead_info ti
JOIN actions a ON a.task_id = ti.task_id
JOIN team_members m ON a.team_member_id = m.id
WHERE a.type = 'request' AND a.is_done = false;

--Doublecheck по правам на таблицы (опционально)
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO birthdaybot;
--GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO birthdaybot;
