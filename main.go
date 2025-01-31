package main

import (
        "database/sql"
        "fmt"
        "log"
        "os"
        "strconv"
        "strings"
        "time"

        tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
        _ "github.com/lib/pq"
)

type Team struct {
        ID       int
        Name     string
        IsActive bool
}

type TeamMember struct {
        ID             int
        Name           string
        Birthday       time.Time
        TeamID         int
        TeamName       string
        PhoneNumber    string
        TelegramChatID int64
}

type YearTask struct {
        ID                 int
        Year              int
        IsMembersNotified bool
        IsTeamleadNotified bool
        IsMoneyTransfered  bool
}

type Action struct {
        ID           int
        TaskID       int
        TeamMemberID int
        Type         string // "request" or "payout"
        IsDone       bool
        MemberName   string
}

type TeamLead struct {
        ID           int
        TeamMemberID int
        TeamID       int
        PhoneNumber  string
        MemberName   string
        TeamName     string
}

type UserState struct {
        Stage       string // "awaiting_name", "awaiting_birthday", "awaiting_phone", "awaiting_team"
        Name        string
        Birthday    time.Time
        PhoneNumber string
}

var userStates = make(map[int64]*UserState)

func main() {
        // Отладочная информация
        log.Printf("Starting bot...")
        log.Printf("Environment variables:")
        log.Printf("DATABASE_URL: %s", os.Getenv("DATABASE_URL"))
        log.Printf("TELEGRAM_BOT_TOKEN: %s", os.Getenv("TELEGRAM_BOT_TOKEN"))

        // Подключение к базе данных
        connStr := os.Getenv("DATABASE_URL")
        if connStr == "" {
                log.Fatal("DATABASE_URL environment variable is not set")
        }

        db, err := sql.Open("postgres", connStr)
        if err != nil {
                log.Fatal(err)
        }
        defer db.Close()

        // Проверка подключения к базе данных
        err = db.Ping()
        if err != nil {
                log.Fatal(err)
        }

        // Установка часового пояса Москвы
        loc, err := time.LoadLocation("Europe/Moscow")
        if err != nil {
                log.Fatal(err)
        }
        time.Local = loc

        // Инициализация бота
        bot, err := tgbotapi.NewBotAPI(os.Getenv("TELEGRAM_BOT_TOKEN"))
        if err != nil {
                log.Fatal(err)
        }

        log.Printf("Authorized on account %s", bot.Self.UserName)

        u := tgbotapi.NewUpdate(0)
        u.Timeout = 60

        updates := bot.GetUpdatesChan(u)

        // Запуск горутин для проверки предстоящих дней рождения и создания actions
        go checkUpcomingBirthdays(db)
        go createRequestActions(db)
        go sendNotifications(db, bot)
        go sendBirthdayWishes(db, bot)
        go sendPayoutReminders(db, bot)

        // Обработка сообщений
        for update := range updates {
                if update.Message != nil {
                        handleMessage(bot, db, update.Message)
                } else if update.CallbackQuery != nil {
                        handleCallback(bot, db, update.CallbackQuery)
                }
        }
}

func handleMessage(bot *tgbotapi.BotAPI, db *sql.DB, message *tgbotapi.Message) {
        userID := message.From.ID
        chatID := message.Chat.ID

        if message.IsCommand() {
                switch message.Command() {
                case "start":
                        // Начинаем процесс регистрации
                        userStates[userID] = &UserState{Stage: "awaiting_name"}
                        msg := tgbotapi.NewMessage(chatID, "Привет! Давайте добавим ваш день рождения в базу данных. Как вас зовут?")
                        bot.Send(msg)
                        return
                case "help":
                        msg := tgbotapi.NewMessage(chatID, `Доступные команды:
/start - начать процесс регистрации
/birthdays - показать ближайшие дни рождения (только для тимлидов)
/help - показать это сообщение`)
                        bot.Send(msg)
                        return
                case "teamleads":
                        teamLeads, err := getTeamLeads(db)
                        if err != nil {
                                log.Printf("Error getting team leads: %v", err)
                                msg := tgbotapi.NewMessage(chatID, "Произошла ошибка при получении списка тимлидов")
                                bot.Send(msg)
                                return
                        }
                        msg := tgbotapi.NewMessage(chatID, formatTeamLeadsMessage(teamLeads))
                        bot.Send(msg)
                        return
                case "birthdays":
                        // Проверяем, является ли пользователь тимлидом
                        var isTeamLead bool
                        err := db.QueryRow(`
                                SELECT EXISTS(
                                        SELECT 1 FROM teamleads 
                                        WHERE team_member_id IN (
                                                SELECT id FROM team_members 
                                                WHERE name = $1
                                        )
                                )`, message.From.UserName).Scan(&isTeamLead)
                        if err != nil {
                                log.Printf("Error checking team lead status: %v", err)
                                return
                        }
                        if !isTeamLead {
                                msg := tgbotapi.NewMessage(chatID, "Эта команда доступна только для тимлидов.")
                                bot.Send(msg)
                                return
                        }

                        birthdays, err := getUpcomingBirthdays(db)
                        if err != nil {
                                log.Printf("Error getting birthdays: %v", err)
                                return
                        }
                        msg := tgbotapi.NewMessage(chatID, formatBirthdayMessage(birthdays))
                        bot.Send(msg)
                        return
                }
        }

        // Обработка состояний пользователя
        state, exists := userStates[userID]
        if !exists {
                msg := tgbotapi.NewMessage(chatID, "Используйте /start для начала процесса регистрации.")
                bot.Send(msg)
                return
        }

        switch state.Stage {
        case "awaiting_name":
                state.Name = message.Text
                state.Stage = "awaiting_birthday"
                msg := tgbotapi.NewMessage(chatID, "Отлично! Теперь введите вашу дату рождения в формате DD.MM.YYYY")
                bot.Send(msg)

        case "awaiting_birthday":
                birthday, err := time.Parse("02.01.2006", message.Text)
                if err != nil {
                        msg := tgbotapi.NewMessage(chatID, "Неверный формат даты. Пожалуйста, используйте формат DD.MM.YYYY")
                        bot.Send(msg)
                        return
                }

                state.Birthday = birthday
                state.Stage = "awaiting_phone"

                // Создаем кнопку для запроса номера телефона
                keyboard := tgbotapi.NewReplyKeyboard(
                        tgbotapi.NewKeyboardButtonRow(
                                tgbotapi.NewKeyboardButtonContact("📱 Поделиться номером телефона"),
                        ),
                )
                keyboard.OneTimeKeyboard = true // Клавиатура исчезнет после использования

                msg := tgbotapi.NewMessage(chatID, "Отлично! Пожалуйста, нажмите на кнопку ниже, чтобы поделиться своим номером телефона")
                msg.ReplyMarkup = keyboard
                bot.Send(msg)
                return

        case "awaiting_phone":
                var msg tgbotapi.MessageConfig
                var err error
                var teams []Team

                // Проверяем, что пользователь отправил контакт, а не текстовое сообщение
                if message.Contact == nil {
                        msg = tgbotapi.NewMessage(chatID, "Пожалуйста, используйте кнопку 'Поделиться номером телефона' для отправки вашего номера")
                        bot.Send(msg)
                        return
                }

                // Проверяем, что контакт принадлежит пользователю
                if message.Contact.UserID != message.From.ID {
                        msg = tgbotapi.NewMessage(chatID, "Пожалуйста, поделитесь своим собственным номером телефона")
                        bot.Send(msg)
                        return
                }

                state.PhoneNumber = message.Contact.PhoneNumber
                state.Stage = "awaiting_team"

                // Убираем клавиатуру после получения номера
                msg = tgbotapi.NewMessage(chatID, "Спасибо! Теперь выберите вашу команду")
                msg.ReplyMarkup = tgbotapi.NewRemoveKeyboard(true)
                bot.Send(msg)

                // Получаем список команд и создаем inline-кнопки
                teams, err = getActiveTeams(db)
                if err != nil {
                        log.Printf("Error getting teams: %v", err)
                        msg = tgbotapi.NewMessage(chatID, "Произошла ошибка при получении списка команд")
                        bot.Send(msg)
                        return
                }

                msg = tgbotapi.NewMessage(chatID, "Выберите вашу команду:")
                var buttons [][]tgbotapi.InlineKeyboardButton
                for _, team := range teams {
                        button := tgbotapi.NewInlineKeyboardButtonData(team.Name, fmt.Sprintf("team_%d", team.ID))
                        buttons = append(buttons, []tgbotapi.InlineKeyboardButton{button})
                }
                msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(buttons...)
                bot.Send(msg)
        }
}

func handleCallback(bot *tgbotapi.BotAPI, db *sql.DB, callback *tgbotapi.CallbackQuery) {
        // Проверяем тип callback
        if strings.HasPrefix(callback.Data, "team_") {
                handleTeamSelection(bot, db, callback)
        } else if strings.HasPrefix(callback.Data, "transfer_done_") {
                handleTransferConfirmation(bot, db, callback)
        } else if strings.HasPrefix(callback.Data, "payout_done_") {
                handlePayoutConfirmation(bot, db, callback)
        }
}

func handleTeamSelection(bot *tgbotapi.BotAPI, db *sql.DB, callback *tgbotapi.CallbackQuery) {
        userID := callback.From.ID
        state, exists := userStates[userID]
        if !exists || state.Stage != "awaiting_team" {
                return
        }

        // Извлекаем ID команды из callback data
        parts := strings.Split(callback.Data, "_")
        if len(parts) != 2 {
                return
        }

        teamID, err := strconv.Atoi(parts[1])
        if err != nil {
                return
        }

        // Добавляем пользователя в базу данных
        err = addBirthday(db, state.Name, state.Birthday, state.PhoneNumber, teamID, callback.Message.Chat.ID)
        if err != nil {
                log.Printf("Error adding birthday: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при сохранении данных")
                bot.Send(msg)
                return
        }

        // Отправляем подтверждение
        msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Спасибо, данные приняты!")
        bot.Send(msg)

        // Удаляем клавиатуру
        edit := tgbotapi.NewEditMessageReplyMarkup(
                callback.Message.Chat.ID,
                callback.Message.MessageID,
                tgbotapi.InlineKeyboardMarkup{})
        bot.Send(edit)

        // Очищаем состояние пользователя
        delete(userStates, userID)
}

func checkUpcomingBirthdays(db *sql.DB) {
        for {
                // Ждем до 00:01 по московскому времени
                now := time.Now()
                next := time.Date(now.Year(), now.Month(), now.Day(), 0, 1, 0, 0, time.Local)
                if now.After(next) {
                        next = next.Add(24 * time.Hour)
                }
                time.Sleep(time.Until(next))

                // Проверяем дни рождения через 3 дня
                query := `
                        WITH birthday_dates AS (
                                SELECT 
                                        id,
                                        birthday,
                                        (CASE 
                                                WHEN (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday))::integer * INTERVAL '1 year')) < CURRENT_DATE
                                                THEN (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday) + 1)::integer * INTERVAL '1 year'))
                                                ELSE (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday))::integer * INTERVAL '1 year'))
                                        END) as next_birthday
                                FROM team_members
                        )
                        SELECT 
                                m.id
                        FROM team_members m
                        JOIN birthday_dates bd ON m.id = bd.id
                        LEFT JOIN year_tasks yt ON 
                                yt.team_member_id = m.id AND 
                                yt.year = EXTRACT(YEAR FROM bd.next_birthday)::integer
                        WHERE bd.next_birthday = CURRENT_DATE + INTERVAL '3 days'
                        AND yt.id IS NULL`

                rows, err := db.Query(query)
                if err != nil {
                        log.Printf("Error checking upcoming birthdays: %v", err)
                        continue
                }

                currentYear := time.Now().Year()
                for rows.Next() {
                        var memberID int
                        if err := rows.Scan(&memberID); err != nil {
                                log.Printf("Error scanning member ID: %v", err)
                                continue
                        }

                        // Создаем новую задачу для дня рождения
                        _, err = db.Exec(`
                                INSERT INTO year_tasks (year, team_member_id)
                                VALUES ($1, $2)`,
                                currentYear, memberID)
                        if err != nil {
                                log.Printf("Error creating year task for member %d: %v", memberID, err)
                        }
                }
                rows.Close()
        }
}

func getTodaysBirthdays(db *sql.DB) ([]TeamMember, error) {
        query := `
                SELECT m.id, m.name, m.birthday, m.team_id, t.name as team_name
                FROM team_members m
                JOIN teams t ON m.team_id = t.id
                WHERE t.is_active = true
                AND EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM CURRENT_DATE)
                AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM CURRENT_DATE)`

        return queryBirthdays(db, query)
}

func getUpcomingBirthdays(db *sql.DB) ([]TeamMember, error) {
        query := `
                WITH birthday_dates AS (
                        SELECT 
                                id,
                                birthday,
                                (CASE 
                                        WHEN (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday))::integer * INTERVAL '1 year')) < CURRENT_DATE
                                        THEN (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday) + 1)::integer * INTERVAL '1 year'))
                                        ELSE (birthday + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthday))::integer * INTERVAL '1 year'))
                                END) as next_birthday
                        FROM team_members
                )
                SELECT 
                        m.id, 
                        m.name, 
                        m.birthday, 
                        m.team_id, 
                        m.phone_number,
                        t.name as team_name
                FROM team_members m
                JOIN teams t ON m.team_id = t.id
                JOIN birthday_dates bd ON m.id = bd.id
                WHERE t.is_active = true
                AND bd.next_birthday <= CURRENT_DATE + INTERVAL '30 days'
                AND bd.next_birthday >= CURRENT_DATE
                ORDER BY bd.next_birthday`

        return queryBirthdays(db, query)
}

func queryBirthdays(db *sql.DB, query string) ([]TeamMember, error) {
        rows, err := db.Query(query)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var birthdays []TeamMember
        for rows.Next() {
                var member TeamMember
                err := rows.Scan(&member.ID, &member.Name, &member.Birthday, &member.TeamID, &member.TeamName)
                if err != nil {
                        return nil, err
                }
                birthdays = append(birthdays, member)
        }

        return birthdays, nil
}

func getActiveTeams(db *sql.DB) ([]Team, error) {
        query := `
                SELECT id, name, is_active
                FROM teams
                WHERE is_active = true
                ORDER BY name`

        rows, err := db.Query(query)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var teams []Team
        for rows.Next() {
                var team Team
                err := rows.Scan(&team.ID, &team.Name, &team.IsActive)
                if err != nil {
                        return nil, err
                }
                teams = append(teams, team)
        }

        return teams, nil
}

func formatBirthdayMessage(birthdays []TeamMember) string {
        if len(birthdays) == 0 {
                return "Нет предстоящих дней рождения."
        }

        msg := "Дни рождения:\n\n"
        for _, member := range birthdays {
                msg += fmt.Sprintf("%s (Команда: %s) - %s\n",
                        member.Name,
                        member.TeamName,
                        member.Birthday.Format("02.01.2006"))
        }
        return msg
}

func addBirthday(db *sql.DB, name string, birthday time.Time, phoneNumber string, teamID int, telegramChatID int64) error {
        // Проверяем существование команды
        var exists bool
        err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM teams WHERE id = $1 AND is_active = true)", teamID).Scan(&exists)
        if err != nil {
                return err
        }
        if !exists {
                return fmt.Errorf("команда с ID %d не существует или не активна", teamID)
        }

        // Добавляем день рождения
        _, err = db.Exec(`
                INSERT INTO team_members (name, birthday, phone_number, team_id, telegram_chat_id)
                VALUES ($1, $2, $3, $4, $5)`,
                name, birthday, phoneNumber, teamID, telegramChatID)
        return err
}

func getTeamLeads(db *sql.DB) ([]TeamLead, error) {
        query := `
                SELECT 
                        tl.id,
                        tl.team_member_id,
                        tl.team_id,
                        tl.phone_number,
                        tm.name as member_name,
                        t.name as team_name
                FROM teamleads tl
                JOIN team_members tm ON tl.team_member_id = tm.id
                JOIN teams t ON tl.team_id = t.id
                WHERE t.is_active = true
                ORDER BY t.name, tm.name`

        rows, err := db.Query(query)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var teamLeads []TeamLead
        for rows.Next() {
                var lead TeamLead
                err := rows.Scan(
                        &lead.ID,
                        &lead.TeamMemberID,
                        &lead.TeamID,
                        &lead.PhoneNumber,
                        &lead.MemberName,
                        &lead.TeamName,
                )
                if err != nil {
                        return nil, err
                }
                teamLeads = append(teamLeads, lead)
        }

        return teamLeads, nil
}

func getTeamLeadByTeamID(db *sql.DB, teamID int) (*TeamLead, error) {
        query := `
                SELECT 
                        tl.id,
                        tl.team_member_id,
                        tl.team_id,
                        tl.phone_number,
                        tm.name as member_name,
                        t.name as team_name
                FROM teamleads tl
                JOIN team_members tm ON tl.team_member_id = tm.id
                JOIN teams t ON tl.team_id = t.id
                WHERE t.is_active = true AND tl.team_id = $1
                LIMIT 1`

        var lead TeamLead
        err := db.QueryRow(query, teamID).Scan(
                &lead.ID,
                &lead.TeamMemberID,
                &lead.TeamID,
                &lead.PhoneNumber,
                &lead.MemberName,
                &lead.TeamName,
        )
        if err == sql.ErrNoRows {
                return nil, nil
        }
        if err != nil {
                return nil, err
        }

        return &lead, nil
}

func addTeamLead(db *sql.DB, teamMemberID, teamID int, phoneNumber string) error {
        // Проверяем существование team_member
        var exists bool
        err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM team_members WHERE id = $1)", teamMemberID).Scan(&exists)
        if err != nil {
                return err
        }
        if !exists {
                return fmt.Errorf("team member с ID %d не существует", teamMemberID)
        }

        // Проверяем существование team
        err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM teams WHERE id = $1 AND is_active = true)", teamID).Scan(&exists)
        if err != nil {
                return err
        }
        if !exists {
                return fmt.Errorf("team с ID %d не существует или не активна", teamID)
        }

        // Проверяем, нет ли уже тимлида для этой команды
        err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM teamleads WHERE team_id = $1)", teamID).Scan(&exists)
        if err != nil {
                return err
        }
        if exists {
                return fmt.Errorf("для команды с ID %d уже назначен тимлид", teamID)
        }

        // Добавляем тимлида
        _, err = db.Exec(`
                INSERT INTO teamleads (team_member_id, team_id, phone_number)
                VALUES ($1, $2, $3)`,
                teamMemberID, teamID, phoneNumber)
        return err
}

func updateTeamLeadPhone(db *sql.DB, teamLeadID int, newPhoneNumber string) error {
        result, err := db.Exec(`
                UPDATE teamleads
                SET phone_number = $1
                WHERE id = $2`,
                newPhoneNumber, teamLeadID)
        if err != nil {
                return err
        }

        rowsAffected, err := result.RowsAffected()
        if err != nil {
                return err
        }
        if rowsAffected == 0 {
                return fmt.Errorf("тимлид с ID %d не найден", teamLeadID)
        }

        return nil
}

func removeTeamLead(db *sql.DB, teamLeadID int) error {
        result, err := db.Exec("DELETE FROM teamleads WHERE id = $1", teamLeadID)
        if err != nil {
                return err
        }

        rowsAffected, err := result.RowsAffected()
        if err != nil {
                return err
        }
        if rowsAffected == 0 {
                return fmt.Errorf("тимлид с ID %d не найден", teamLeadID)
        }

        return nil
}

func createRequestActions(db *sql.DB) {
        for {
                // Ждем до 00:10 по московскому времени
                now := time.Now()
                next := time.Date(now.Year(), now.Month(), now.Day(), 0, 10, 0, 0, time.Local)
                if now.After(next) {
                        next = next.Add(24 * time.Hour)
                }
                time.Sleep(time.Until(next))

                // Находим задачи без actions типа request
                query := `
                        SELECT yt.id, yt.team_member_id
                        FROM year_tasks yt
                        LEFT JOIN actions a ON a.task_id = yt.id AND a.type = 'request'
                        WHERE a.id IS NULL`

                rows, err := db.Query(query)
                if err != nil {
                        log.Printf("Error querying tasks without actions: %v", err)
                        continue
                }

                for rows.Next() {
                        var taskID, birthdayMemberID int
                        if err := rows.Scan(&taskID, &birthdayMemberID); err != nil {
                                log.Printf("Error scanning task: %v", err)
                                continue
                        }

                        // Получаем всех членов команды, кроме именинника
                        memberRows, err := db.Query(`
                                SELECT id 
                                FROM team_members 
                                WHERE id != $1`,
                                birthdayMemberID)
                        if err != nil {
                                log.Printf("Error getting team members: %v", err)
                                continue
                        }

                        // Создаем actions для каждого члена команды
                        for memberRows.Next() {
                                var memberID int
                                if err := memberRows.Scan(&memberID); err != nil {
                                        log.Printf("Error scanning member: %v", err)
                                        continue
                                }

                                _, err = db.Exec(`
                                        INSERT INTO actions (task_id, team_member_id, type)
                                        VALUES ($1, $2, 'request')`,
                                        taskID, memberID)
                                if err != nil {
                                        log.Printf("Error creating action for member %d: %v", memberID, err)
                                }
                        }
                        memberRows.Close()
                }
                rows.Close()
        }
}

func handleTransferConfirmation(bot *tgbotapi.BotAPI, db *sql.DB, callback *tgbotapi.CallbackQuery) {
        // Извлекаем ID действия из callback data
        parts := strings.Split(callback.Data, "_")
        if len(parts) != 3 {
                return
        }

        actionID, err := strconv.Atoi(parts[2])
        if err != nil {
                return
        }

        // Обновляем статус действия
        err = updateActionStatus(db, actionID, true)
        if err != nil {
                log.Printf("Error updating action status: %v", err)
                return
        }

        // Отправляем подтверждение
        msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Спасибо! Статус обновлен.")
        bot.Send(msg)

        // Удаляем кнопку
        edit := tgbotapi.NewEditMessageReplyMarkup(
                callback.Message.Chat.ID,
                callback.Message.MessageID,
                tgbotapi.InlineKeyboardMarkup{})
        bot.Send(edit)
}

func handlePayoutConfirmation(bot *tgbotapi.BotAPI, db *sql.DB, callback *tgbotapi.CallbackQuery) {
        // Извлекаем ID действия и задачи из callback data
        parts := strings.Split(callback.Data, "_")
        if len(parts) != 4 {
                return
        }

        actionID, err := strconv.Atoi(parts[2])
        if err != nil {
                return
        }

        taskID, err := strconv.Atoi(parts[3])
        if err != nil {
                return
        }

        // Начинаем транзакцию
        tx, err := db.Begin()
        if err != nil {
                log.Printf("Error starting transaction: %v", err)
                return
        }

        // Обновляем статус действия
        _, err = tx.Exec(`
                UPDATE actions 
                SET is_done = true 
                WHERE id = $1`,
                actionID)
        if err != nil {
                tx.Rollback()
                log.Printf("Error updating action status: %v", err)
                return
        }

        // Обновляем статус задачи
        _, err = tx.Exec(`
                UPDATE year_tasks 
                SET is_money_transfered = true 
                WHERE id = $1`,
                taskID)
        if err != nil {
                tx.Rollback()
                log.Printf("Error updating task status: %v", err)
                return
        }

        // Подтверждаем транзакцию
        err = tx.Commit()
        if err != nil {
                log.Printf("Error committing transaction: %v", err)
                return
        }

        // Отправляем подтверждение
        msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Спасибо! Подарок отправлен имениннику.")
        bot.Send(msg)

        // Удаляем кнопку
        edit := tgbotapi.NewEditMessageReplyMarkup(
                callback.Message.Chat.ID,
                callback.Message.MessageID,
                tgbotapi.InlineKeyboardMarkup{})
        bot.Send(edit)
}

func sendNotifications(db *sql.DB, bot *tgbotapi.BotAPI) {
        for {
                now := time.Now()

                // Ждем до 8:00 для отправки уведомлений участникам
                next := time.Date(now.Year(), now.Month(), now.Day(), 8, 0, 0, 0, time.Local)
                if now.After(next) {
                        next = next.Add(24 * time.Hour)
                }
                time.Sleep(time.Until(next))

                // Отправляем уведомления участникам
                sendMemberNotifications(db, bot)

                // Ждем 5 минут
                time.Sleep(5 * time.Minute)

                // Отправляем уведомления тимлидам
                sendTeamLeadNotifications(db, bot)
        }
}

func sendMemberNotifications(db *sql.DB, bot *tgbotapi.BotAPI) {
        query := `
                WITH birthday_info AS (
                        SELECT 
                                yt.id as task_id,
                                bm.name as birthday_person_name,
                                t.name as team_name,
                                tl.phone_number as teamlead_phone,
                                tlm.name as teamlead_name
                        FROM year_tasks yt
                        JOIN team_members bm ON yt.team_member_id = bm.id
                        JOIN teams t ON bm.team_id = t.id
                        JOIN teamleads tl ON t.id = tl.team_id
                        JOIN team_members tlm ON tl.team_member_id = tlm.id
                        WHERE yt.is_members_notified = false
                )
                SELECT 
                        a.id as action_id,
                        bi.*,
                        m.telegram_chat_id
                FROM birthday_info bi
                JOIN actions a ON a.task_id = bi.task_id
                JOIN team_members m ON a.team_member_id = m.id
                WHERE a.type = 'request' AND a.is_done = false`

        rows, err := db.Query(query)
        if err != nil {
                log.Printf("Error querying for member notifications: %v", err)
                return
        }
        defer rows.Close()

        for rows.Next() {
                var (
                        actionID        int
                        taskID         int
                        birthdayName   string
                        teamName       string
                        teamleadPhone  string
                        teamleadName   string
                        telegramChatID int64
                )

                err := rows.Scan(&actionID, &taskID, &birthdayName, &teamName, &teamleadPhone, &teamleadName, &telegramChatID)
                if err != nil {
                        log.Printf("Error scanning member notification data: %v", err)
                        continue
                }

                // Создаем сообщение с кнопкой
                keyboard := tgbotapi.NewInlineKeyboardMarkup(
                        tgbotapi.NewInlineKeyboardRow(
                                tgbotapi.NewInlineKeyboardButtonData("Готово, перевел", fmt.Sprintf("transfer_done_%d", actionID)),
                        ),
                )

                msg := tgbotapi.NewMessage(telegramChatID,
                        fmt.Sprintf("Привет! Через 3 дня %s из команды %s празднует день рождения! "+
                                "Переведи, пожалуйста, свой вклад в подарок нашему коллеге по номеру телефона %s, получатель %s.",
                                birthdayName, teamName, teamleadPhone, teamleadName))
                msg.ReplyMarkup = keyboard

                _, err = bot.Send(msg)
                if err != nil {
                        log.Printf("Error sending member notification: %v", err)
                }
        }

        // Обновляем статус уведомлений
        _, err = db.Exec(`
                UPDATE year_tasks 
                SET is_members_notified = true 
                WHERE is_members_notified = false`)
        if err != nil {
                log.Printf("Error updating members notification status: %v", err)
        }
}

func sendTeamLeadNotifications(db *sql.DB, bot *tgbotapi.BotAPI) {
        query := `
                SELECT 
                        yt.id as task_id,
                        bm.name as birthday_person_name,
                        tlm.telegram_chat_id
                FROM year_tasks yt
                JOIN team_members bm ON yt.team_member_id = bm.id
                JOIN teams t ON bm.team_id = t.id
                JOIN teamleads tl ON t.id = tl.team_id
                JOIN team_members tlm ON tl.team_member_id = tlm.id
                WHERE yt.is_teamlead_notified = false`

        rows, err := db.Query(query)
        if err != nil {
                log.Printf("Error querying for teamlead notifications: %v", err)
                return
        }
        defer rows.Close()

        for rows.Next() {
                var (
                        taskID         int
                        birthdayName   string
                        telegramChatID int64
                )

                err := rows.Scan(&taskID, &birthdayName, &telegramChatID)
                if err != nil {
                        log.Printf("Error scanning teamlead notification data: %v", err)
                        continue
                }

                msg := tgbotapi.NewMessage(telegramChatID,
                        fmt.Sprintf("%s празднует день рождения через 3 дня! "+
                                "Сейчас тебе начнут поступать переводы ему на подарок! "+
                                "Не забудь запланировать поздравление!", birthdayName))

                _, err = bot.Send(msg)
                if err != nil {
                        log.Printf("Error sending teamlead notification: %v", err)
                        continue
                }

                // Обновляем статус уведомления для этой задачи
                _, err = db.Exec(`
                        UPDATE year_tasks 
                        SET is_teamlead_notified = true 
                        WHERE id = $1`,
                        taskID)
                if err != nil {
                        log.Printf("Error updating teamlead notification status: %v", err)
                }
        }
}

func sendBirthdayWishes(db *sql.DB, bot *tgbotapi.BotAPI) {
        for {
                // Ждем до 8:10 по московскому времени
                now := time.Now()
                next := time.Date(now.Year(), now.Month(), now.Day(), 8, 10, 0, 0, time.Local)
                if now.After(next) {
                        next = next.Add(24 * time.Hour)
                }
                time.Sleep(time.Until(next))

                // Находим именинников
                query := `
                        SELECT 
                                m.id,
                                m.name,
                                m.telegram_chat_id,
                                t.id as team_id,
                                yt.id as task_id,
                                tl.team_member_id as teamlead_id
                        FROM team_members m
                        JOIN teams t ON m.team_id = t.id
                        JOIN teamleads tl ON t.id = tl.team_id
                        LEFT JOIN year_tasks yt ON 
                                yt.team_member_id = m.id AND 
                                yt.year = EXTRACT(YEAR FROM CURRENT_DATE)::integer
                        WHERE EXTRACT(MONTH FROM m.birthday) = EXTRACT(MONTH FROM CURRENT_DATE)
                        AND EXTRACT(DAY FROM m.birthday) = EXTRACT(DAY FROM CURRENT_DATE)`

                rows, err := db.Query(query)
                if err != nil {
                        log.Printf("Error querying birthday people: %v", err)
                        continue
                }

                for rows.Next() {
                        var (
                                memberID       int
                                name          string
                                telegramChatID int64
                                teamID        int
                                taskID        sql.NullInt64
                                teamleadID    int
                        )

                        err := rows.Scan(&memberID, &name, &telegramChatID, &teamID, &taskID, &teamleadID)
                        if err != nil {
                                log.Printf("Error scanning birthday person data: %v", err)
                                continue
                        }

                        // Отправляем поздравление
                        msg := tgbotapi.NewMessage(telegramChatID,
                                "Привет! Сегодня твой день рождения и, от имени всей команды, "+
                                        "я поздравляю тебя с этим замечательным праздником! "+
                                        "Пусть тебе сопутствуют успех, удача и здоровье!")
                        
                        _, err = bot.Send(msg)
                        if err != nil {
                                log.Printf("Error sending birthday wish: %v", err)
                        }

                        // Создаем action для тимлида, если есть task
                        if taskID.Valid {
                                _, err = db.Exec(`
                                        INSERT INTO actions (task_id, team_member_id, type)
                                        VALUES ($1, $2, 'payout')`,
                                        taskID.Int64, teamleadID)
                                if err != nil {
                                        log.Printf("Error creating payout action: %v", err)
                                }
                        }
                }
                rows.Close()
        }
}

func sendPayoutReminders(db *sql.DB, bot *tgbotapi.BotAPI) {
        for {
                // Ждем до 9:00 по московскому времени
                now := time.Now()
                next := time.Date(now.Year(), now.Month(), now.Day(), 9, 0, 0, 0, time.Local)
                if now.After(next) {
                        next = next.Add(24 * time.Hour)
                }
                time.Sleep(time.Until(next))

                query := `
                        SELECT 
                                a.id as action_id,
                                tl.telegram_chat_id as teamlead_chat_id,
                                bm.name as birthday_person_name,
                                bm.phone_number as birthday_person_phone,
                                yt.id as task_id
                        FROM actions a
                        JOIN year_tasks yt ON a.task_id = yt.id
                        JOIN team_members bm ON yt.team_member_id = bm.id
                        JOIN team_members tl ON a.team_member_id = tl.id
                        WHERE a.type = 'payout' 
                        AND a.is_done = false
                        AND yt.is_money_transfered = false`

                rows, err := db.Query(query)
                if err != nil {
                        log.Printf("Error querying payout reminders: %v", err)
                        continue
                }

                for rows.Next() {
                        var (
                                actionID           int
                                teamleadChatID     int64
                                birthdayPersonName string
                                birthdayPersonPhone string
                                taskID             int
                        )

                        err := rows.Scan(&actionID, &teamleadChatID, &birthdayPersonName, &birthdayPersonPhone, &taskID)
                        if err != nil {
                                log.Printf("Error scanning payout reminder data: %v", err)
                                continue
                        }

                        // Создаем сообщение с кнопкой
                        keyboard := tgbotapi.NewInlineKeyboardMarkup(
                                tgbotapi.NewInlineKeyboardRow(
                                        tgbotapi.NewInlineKeyboardButtonData("Готово, перевел", fmt.Sprintf("payout_done_%d_%d", actionID, taskID)),
                                ),
                        )

                        msg := tgbotapi.NewMessage(teamleadChatID,
                                fmt.Sprintf("Привет! Нужно перевести подарок имениннику! "+
                                        "Получатель %s, номер телефона %s",
                                        birthdayPersonName, birthdayPersonPhone))
                        msg.ReplyMarkup = keyboard

                        _, err = bot.Send(msg)
                        if err != nil {
                                log.Printf("Error sending payout reminder: %v", err)
                        }
                }
                rows.Close()
        }
}

func formatTeamLeadsMessage(teamLeads []TeamLead) string {
        if len(teamLeads) == 0 {
                return "Нет назначенных тимлидов."
        }

        msg := "Список тимлидов:\n\n"
        for _, lead := range teamLeads {
                msg += fmt.Sprintf("Команда: %s\nТимлид: %s\nТелефон: %s\n\n",
                        lead.TeamName,
                        lead.MemberName,
                        lead.PhoneNumber)
        }
        return msg
}

// Функции для работы с year_tasks
func createYearTask(db *sql.DB, year int) error {
        _, err := db.Exec(`
                INSERT INTO year_tasks (year)
                VALUES ($1)`,
                year)
        return err
}

func getYearTask(db *sql.DB, year int) (*YearTask, error) {
        var task YearTask
        err := db.QueryRow(`
                SELECT id, year, is_members_notified, is_teamlead_notified, is_money_transfered
                FROM year_tasks
                WHERE year = $1`,
                year).Scan(
                &task.ID,
                &task.Year,
                &task.IsMembersNotified,
                &task.IsTeamleadNotified,
                &task.IsMoneyTransfered)

        if err == sql.ErrNoRows {
                return nil, nil
        }
        if err != nil {
                return nil, err
        }
        return &task, nil
}

func updateYearTaskStatus(db *sql.DB, taskID int, field string, value bool) error {
        query := fmt.Sprintf("UPDATE year_tasks SET %s = $1 WHERE id = $2", field)
        result, err := db.Exec(query, value, taskID)
        if err != nil {
                return err
        }

        rowsAffected, err := result.RowsAffected()
        if err != nil {
                return err
        }
        if rowsAffected == 0 {
                return fmt.Errorf("задача с ID %d не найдена", taskID)
        }
        return nil
}

// Функции для работы с actions
func createAction(db *sql.DB, taskID, teamMemberID int, actionType string) error {
        _, err := db.Exec(`
                INSERT INTO actions (task_id, team_member_id, type)
                VALUES ($1, $2, $3)`,
                taskID, teamMemberID, actionType)
        return err
}

func getActions(db *sql.DB, taskID int) ([]Action, error) {
        rows, err := db.Query(`
                SELECT a.id, a.task_id, a.team_member_id, a.type, a.is_done, m.name
                FROM actions a
                JOIN team_members m ON a.team_member_id = m.id
                WHERE a.task_id = $1
                ORDER BY a.id`,
                taskID)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var actions []Action
        for rows.Next() {
                var action Action
                err := rows.Scan(
                        &action.ID,
                        &action.TaskID,
                        &action.TeamMemberID,
                        &action.Type,
                        &action.IsDone,
                        &action.MemberName)
                if err != nil {
                        return nil, err
                }
                actions = append(actions, action)
        }
        return actions, nil
}

func updateActionStatus(db *sql.DB, actionID int, isDone bool) error {
        result, err := db.Exec(`
                UPDATE actions
                SET is_done = $1
                WHERE id = $2`,
                isDone, actionID)
        if err != nil {
                return err
        }

        rowsAffected, err := result.RowsAffected()
        if err != nil {
                return err
        }
        if rowsAffected == 0 {
                return fmt.Errorf("действие с ID %d не найдено", actionID)
        }
        return nil
}

func getActionsByMember(db *sql.DB, teamMemberID int) ([]Action, error) {
        rows, err := db.Query(`
                SELECT a.id, a.task_id, a.team_member_id, a.type, a.is_done, m.name
                FROM actions a
                JOIN team_members m ON a.team_member_id = m.id
                WHERE a.team_member_id = $1
                ORDER BY a.id`,
                teamMemberID)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var actions []Action
        for rows.Next() {
                var action Action
                err := rows.Scan(
                        &action.ID,
                        &action.TaskID,
                        &action.TeamMemberID,
                        &action.Type,
                        &action.IsDone,
                        &action.MemberName)
                if err != nil {
                        return nil, err
                }
                actions = append(actions, action)
        }
        return actions, nil
}