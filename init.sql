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

CREATE TYPE action_type AS ENUM ('request', 'payout');

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