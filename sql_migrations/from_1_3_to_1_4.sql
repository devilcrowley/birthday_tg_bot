-- Функция для получения альтернативного тимлида
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

-- Обновляем запрос для уведомлений тимлида
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

-- Обновляем запрос для уведомлений участников
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
