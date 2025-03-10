package main

import (
        "database/sql"
        "encoding/json"
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

// Вспомогательная функция для записи в журнал
func logMessageToJournal(db *sql.DB, messageData map[string]interface{}, actionID sql.NullInt64) error {
    jsonBytes, err := json.Marshal(messageData)
    if err != nil {
        return fmt.Errorf("error marshaling message to JSON: %v", err)
    }

    query := `
        INSERT INTO api_messages_journal (message, action_id)
        VALUES ($1::jsonb, $2)`

    _, err = db.Exec(query, string(jsonBytes), actionID)
    if err != nil {
        return fmt.Errorf("error inserting into journal: %v", err)
    }

    return nil
}

// Функция создания записи в журнале для уведомления участника
func createMemberNotificationJournal(db *sql.DB, sentMessage tgbotapi.Message, messageText string, keyboard interface{},
    birthdayName, teamName, teamleadName, teamleadPhone string, actionID int) error {
    messageJSON := map[string]interface{}{
        "message_id": sentMessage.MessageID,
        "chat_id": sentMessage.Chat.ID,
        "text": messageText,
        "keyboard": keyboard,
        "type": "member_notification",
        "birthday_person": map[string]interface{}{
            "name": birthdayName,
            "team": teamName,
        },
        "teamlead": map[string]interface{}{
            "name": teamleadName,
            "phone": teamleadPhone,
        },
    }
    return logMessageToJournal(db, messageJSON, sql.NullInt64{Int64: int64(actionID), Valid: true})
}

// Функция создания записи в журнале для уведомления тимлида
func createTeamLeadNotificationJournal(db *sql.DB, sentMessage tgbotapi.Message, messageText string,
    birthdayName string, taskID int) error {
    messageJSON := map[string]interface{}{
        "message_id": sentMessage.MessageID,
        "chat_id": sentMessage.Chat.ID,
        "text": messageText,
        "type": "teamlead_notification",
        "birthday_person": map[string]interface{}{
            "name": birthdayName,
        },
        "task_id": taskID,
    }
    return logMessageToJournal(db, messageJSON, sql.NullInt64{Valid: false})
}

// Функция создания записи в журнале для поздравления с днем рождения
func createBirthdayWishJournal(db *sql.DB, sentMessage tgbotapi.Message, messageText string,
    memberID int, name string, teamID int) error {
    messageJSON := map[string]interface{}{
        "message_id": sentMessage.MessageID,
        "chat_id": sentMessage.Chat.ID,
        "text": messageText,
        "type": "birthday_wish",
        "birthday_person": map[string]interface{}{
            "id": memberID,
            "name": name,
            "team_id": teamID,
        },
    }
    return logMessageToJournal(db, messageJSON, sql.NullInt64{Valid: false})
}

// Функция создания записи в журнале для напоминания о переводе денег
func createPayoutReminderJournal(db *sql.DB, sentMessage tgbotapi.Message, messageText string, keyboard interface{},
    birthdayPersonName, birthdayPersonPhone string, actionID int) error {
    messageJSON := map[string]interface{}{
        "message_id": sentMessage.MessageID,
        "chat_id": sentMessage.Chat.ID,
        "text": messageText,
        "keyboard": keyboard,
        "type": "payout_reminder",
        "birthday_person": map[string]interface{}{
            "name": birthdayPersonName,
            "phone": birthdayPersonPhone,
        },
    }
    return logMessageToJournal(db, messageJSON, sql.NullInt64{Int64: int64(actionID), Valid: true})
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
                    SELECT 1 FROM teamleads tl
                    JOIN team_members tm ON tl.team_member_id = tm.id
                    WHERE tm.telegram_chat_id = $1::bigint
                )`, message.Chat.ID).Scan(&isTeamLead)
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
        case "admin":
            // Проверяем, является ли пользователь администратором
            isAdmin, err := isAdmin(db, chatID)
            if err != nil {
                log.Printf("Error checking admin status: %v", err)
                return
            }
            if !isAdmin {
                msg := tgbotapi.NewMessage(chatID, "Эта команда доступна только для администраторов.")
                bot.Send(msg)
                return
            }

            // Создаем inline-кнопки для панели управления
            keyboard := tgbotapi.NewInlineKeyboardMarkup(
                tgbotapi.NewInlineKeyboardRow(
                    tgbotapi.NewInlineKeyboardButtonData("Gen tasks", "admin_gen_tasks"),
                    tgbotapi.NewInlineKeyboardButtonData("Gen actions", "admin_gen_actions"),
                ),
                tgbotapi.NewInlineKeyboardRow(
                    tgbotapi.NewInlineKeyboardButtonData("Send members messages", "admin_send_members_messages"),
                    tgbotapi.NewInlineKeyboardButtonData("Send teamlead notify", "admin_send_teamlead_notify"),
                ),
                tgbotapi.NewInlineKeyboardRow(
                    tgbotapi.NewInlineKeyboardButtonData("Send today birthday messages", "admin_send_today_birthday_messages"),
                    tgbotapi.NewInlineKeyboardButtonData("Send teamlead money message", "admin_send_teamlead_money_message"),
                ),
            )

            msg := tgbotapi.NewMessage(chatID, "Панель управления администратора:")
            msg.ReplyMarkup = keyboard
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
    // Обновляем запись в журнале для любого callback
    if callback.Message != nil {
        updateQuery := `
            UPDATE api_messages_journal
            SET updated_at = CURRENT_TIMESTAMP,
                message = jsonb_set(
                    jsonb_set(message, '{callback_data}', $1::jsonb),
                    '{callback_time}', $2::jsonb
                )
            WHERE message->>'message_id' = $3
            AND message->>'chat_id' = $4`

        callbackJSON, _ := json.Marshal(callback.Data)
        timeJSON, _ := json.Marshal(time.Now().Format(time.RFC3339))
        
        _, err := db.Exec(updateQuery, 
            string(callbackJSON), 
            string(timeJSON),
            strconv.Itoa(callback.Message.MessageID),
            strconv.FormatInt(callback.Message.Chat.ID, 10))
        
        if err != nil {
            log.Printf("Error updating message journal: %v", err)
        }
    }

    // Проверяем тип callback
    if strings.HasPrefix(callback.Data, "team_") {
        handleTeamSelection(bot, db, callback)
    } else if strings.HasPrefix(callback.Data, "transfer_done_") {
        handleTransferConfirmation(bot, db, callback)
    } else if strings.HasPrefix(callback.Data, "payout_done_") {
        handlePayoutConfirmation(bot, db, callback)
    } else if strings.HasPrefix(callback.Data, "admin_") {
        handleAdminCallback(bot, db, callback)
    }
}

func handleAdminCallback(bot *tgbotapi.BotAPI, db *sql.DB, callback *tgbotapi.CallbackQuery) {
    // Проверяем, является ли пользователь администратором
    isAdmin, err := isAdmin(db, callback.Message.Chat.ID)
    if err != nil {
        log.Printf("Error checking admin status: %v", err)
        return
    }
    if !isAdmin {
        msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Эта команда доступна только для администраторов.")
        bot.Send(msg)
        return
    }

    // Отправляем начальное сообщение о начале обработки
    msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Начинаем обработку запроса...")
    bot.Send(msg)

    // Обрабатываем callback в зависимости от типа действия
    switch callback.Data {
    case "admin_gen_tasks":
        go func() {
            log.Printf("Starting to check upcoming birthdays")
            count, err := checkUpcomingBirthdaysCount(db)
            if err != nil {
                log.Printf("Error checking upcoming birthdays count: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке предстоящих дней рождения.")
                bot.Send(msg)
                return
            }
            log.Printf("Found %d upcoming birthdays without tasks", count)
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых дней рождения для создания задач.")
                bot.Send(msg)
                return
            }
            log.Printf("Starting to create birthday tasks")
            tasksCreated, err := checkUpcomingBirthdaysOnce(db)
            if err != nil {
                if err.Error() == "no new tasks created" {
                    msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых дней рождения для создания задач.")
                    bot.Send(msg)
                } else {
                    log.Printf("Error creating birthday tasks: %v", err)
                    msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при создании задач.")
                    bot.Send(msg)
                }
                return
            }
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Задачи успешно созданы для %d предстоящих дней рождения.", tasksCreated))
            bot.Send(msg)
            log.Printf("Finished creating birthday tasks")
        }()
    case "admin_gen_actions":
        go func() {
            log.Printf("Starting to check pending actions")
            count, err := checkPendingActionsCount(db)
            if err != nil {
                log.Printf("Error checking pending actions: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке ожидающих действий.")
                bot.Send(msg)
                return
            }
            log.Printf("Found %d tasks without actions", count)
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых задач для создания действий.")
                bot.Send(msg)
                return
            }
            log.Printf("Starting to create request actions")
            err = createRequestActionsOnce(db)
            if err != nil {
                if err.Error() == "no new actions created" {
                    msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых задач для создания действий.")
                    bot.Send(msg)
                } else {
                    log.Printf("Error creating request actions: %v", err)
                    msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при создании действий.")
                    bot.Send(msg)
                }
                return
            }
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Действия успешно созданы для %d задач.", count))
            bot.Send(msg)
            log.Printf("Finished creating request actions")
        }()
    case "admin_send_members_messages":
        go func() {
            count, err := checkPendingMemberNotificationsCount(db)
            if err != nil {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке ожидающих уведомлений участников.")
                bot.Send(msg)
                return
            }
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых уведомлений для отправки участникам.")
                bot.Send(msg)
                return
            }
            sendMemberNotifications(db, bot)
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Уведомления успешно отправлены %d участникам.", count))
            bot.Send(msg)
        }()
    case "admin_send_teamlead_notify":
        go func() {
            count, err := checkPendingTeamLeadNotificationsCount(db)
            if err != nil {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке ожидающих уведомлений тимлидов.")
                bot.Send(msg)
                return
            }
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых уведомлений для отправки тимлидам.")
                bot.Send(msg)
                return
            }
            sendTeamLeadNotifications(db, bot)
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Уведомления успешно отправлены %d тимлидам.", count))
            bot.Send(msg)
        }()
    case "admin_send_today_birthday_messages":
        go func() {
            log.Printf("Starting to check today's birthdays")
            count, err := checkTodayBirthdaysCount(db)
            if err != nil {
                log.Printf("Error checking today's birthdays: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке сегодняшних дней рождения.")
                bot.Send(msg)
                return
            }
            log.Printf("Found %d birthdays today", count)
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Сегодня нет дней рождения для отправки поздравлений.")
                bot.Send(msg)
                return
            }
            log.Printf("Starting to send birthday wishes")
            err = sendBirthdayWishesOnce(db, bot)
            if err != nil {
                log.Printf("Error sending birthday wishes: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при отправке поздравлений.")
                bot.Send(msg)
                return
            }
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Поздравления успешно отправлены %d именинникам.", count))
            bot.Send(msg)
            log.Printf("Finished sending birthday wishes")
        }()
    case "admin_send_teamlead_money_message":
        go func() {
            log.Printf("Starting to check pending payout reminders")
            count, err := checkPendingPayoutRemindersCount(db)
            if err != nil {
                log.Printf("Error checking pending payout reminders: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при проверке ожидающих напоминаний о переводе денег.")
                bot.Send(msg)
                return
            }
            log.Printf("Found %d pending payout reminders", count)
            if count == 0 {
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Нет новых напоминаний о переводе денег для отправки.")
                bot.Send(msg)
                return
            }
            log.Printf("Starting to send payout reminders")
            err = sendPayoutRemindersOnce(db, bot)
            if err != nil {
                log.Printf("Error sending payout reminders: %v", err)
                msg := tgbotapi.NewMessage(callback.Message.Chat.ID, "Произошла ошибка при отправке напоминаний о переводе денег.")
                bot.Send(msg)
                return
            }
            msg := tgbotapi.NewMessage(callback.Message.Chat.ID, fmt.Sprintf("Напоминания о переводе денег успешно отправлены %d тимлидам.", count))
            bot.Send(msg)
            log.Printf("Finished sending payout reminders")
        }()
    }
}

// Функции проверки количества событий
func checkUpcomingBirthdaysCount(db *sql.DB) (int, error) {
    // Сначала выведем отладочную информацию
    debugQuery := `
        SELECT
            m.id,
            m.name,
            m.birthday,
            CURRENT_DATE as today,
            CURRENT_DATE + INTERVAL '3 days' as max_date,
            EXTRACT(MONTH FROM m.birthday) as birth_month,
            EXTRACT(DAY FROM m.birthday) as birth_day
        FROM team_members m
        WHERE m.id IN (13, 14)`

    debugRows, err := db.Query(debugQuery)
    if err != nil {
        log.Printf("Debug query error: %v", err)
    } else {
        defer debugRows.Close()
        log.Printf("Debug information for users:")
        for debugRows.Next() {
            var (
                id int
                name string
                birthday time.Time
                today, maxDate time.Time
                birthMonth, birthDay int
            )
            if err := debugRows.Scan(&id, &name, &birthday, &today, &maxDate, &birthMonth, &birthDay); err != nil {
                log.Printf("Error scanning debug row: %v", err)
                continue
            }
            log.Printf("ID: %d, Name: %s, Birthday: %v, Today: %v, MaxDate: %v, Birth Month/Day: %d/%d",
                id, name, birthday, today, maxDate, birthMonth, birthDay)
        }
    }

    // Теперь выполним основной запрос для подсчета
    var count int
    query := `
        WITH date_range AS (
            SELECT generate_series(
                CURRENT_DATE,
                CURRENT_DATE + INTERVAL '3 days',
                INTERVAL '1 day'
            )::date as check_date
        )
        SELECT COUNT(DISTINCT m.id)
        FROM team_members m
        CROSS JOIN date_range d
        LEFT JOIN year_tasks yt ON
            yt.team_member_id = m.id AND
            yt.year = EXTRACT(YEAR FROM d.check_date)::integer
        WHERE 
            EXTRACT(MONTH FROM m.birthday) = EXTRACT(MONTH FROM d.check_date)
            AND EXTRACT(DAY FROM m.birthday) = EXTRACT(DAY FROM d.check_date)
            AND yt.id IS NULL`

    err = db.QueryRow(query).Scan(&count)
    log.Printf("Found %d birthdays in range without tasks", count)
    return count, err
}

func checkPendingActionsCount(db *sql.DB) (int, error) {
    var count int
    query := `
        SELECT COUNT(*)
        FROM year_tasks yt
        LEFT JOIN actions a ON yt.id = a.task_id
        WHERE a.id IS NULL
        AND yt.is_members_notified = false`

    err := db.QueryRow(query).Scan(&count)
    return count, err
}

func checkPendingMemberNotificationsCount(db *sql.DB) (int, error) {
    var count int
    query := `
        SELECT COUNT(*)
        FROM year_tasks yt
        WHERE yt.is_members_notified = false
        AND EXISTS (
            SELECT 1 FROM actions a
            WHERE a.task_id = yt.id
            AND a.type = 'request'
            AND a.is_done = false
        )`

    err := db.QueryRow(query).Scan(&count)
    return count, err
}

func checkPendingTeamLeadNotificationsCount(db *sql.DB) (int, error) {
    var count int
    query := `
        SELECT COUNT(*)
        FROM year_tasks yt
        WHERE yt.is_teamlead_notified = false
        AND EXISTS (
            SELECT 1 FROM actions a
            WHERE a.task_id = yt.id
            AND a.type = 'request'
            AND a.is_done = true
        )`

    err := db.QueryRow(query).Scan(&count)
    return count, err
}

func checkTodayBirthdaysCount(db *sql.DB) (int, error) {
    var count int
    query := `
        SELECT COUNT(*)
        FROM team_members
        WHERE EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM CURRENT_DATE)
        AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM CURRENT_DATE)`

    err := db.QueryRow(query).Scan(&count)
    return count, err
}

func checkPendingPayoutRemindersCount(db *sql.DB) (int, error) {
    var count int
    query := `
        SELECT COUNT(DISTINCT yt.id)
        FROM year_tasks yt
        JOIN actions a ON a.task_id = yt.id
        WHERE yt.is_money_transfered = false
        AND a.type = 'payout'
        AND a.is_done = false
        AND yt.is_teamlead_notified = true`

    err := db.QueryRow(query).Scan(&count)
    if err != nil {
        log.Printf("Error in checkPendingPayoutRemindersCount: %v", err)
        return 0, err
    }
    log.Printf("Found %d pending payout reminders", count)
    return count, nil
}

func sendPayoutRemindersOnce(db *sql.DB, bot *tgbotapi.BotAPI) error {
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
        return fmt.Errorf("error querying payout reminders: %v", err)
    }
    defer rows.Close()

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

        messageText := fmt.Sprintf("Напоминание: необходимо перевести деньги %s (тел: %s)", 
            birthdayPersonName, birthdayPersonPhone)
        msg := tgbotapi.NewMessage(teamleadChatID, messageText)
        msg.ReplyMarkup = keyboard

        // Отправляем сообщение
        sentMessage, err := bot.Send(msg)
        if err != nil {
            log.Printf("Error sending payout reminder to teamlead %d: %v", teamleadChatID, err)
            continue
        }
        
        if err := createPayoutReminderJournal(db, sentMessage, messageText, keyboard, birthdayPersonName, birthdayPersonPhone, actionID); err != nil {
            log.Printf("Error logging message to journal: %v", err)
        }
    }

    if err = rows.Err(); err != nil {
        return fmt.Errorf("error iterating over payout reminders: %v", err)
    }

    return nil
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

func checkUpcomingBirthdaysOnce(db *sql.DB) (int, error) {
    // Проверяем дни рождения через 3 дня
    query := `
        WITH date_range AS (
            SELECT generate_series(
                CURRENT_DATE,
                CURRENT_DATE + INTERVAL '3 days',
                INTERVAL '1 day'
            )::date as check_date
        )
        SELECT DISTINCT
            m.id,
            m.name,
            m.birthday,
            d.check_date,
            EXTRACT(MONTH FROM m.birthday) as birth_month,
            EXTRACT(DAY FROM m.birthday) as birth_day,
            EXTRACT(MONTH FROM d.check_date) as check_month,
            EXTRACT(DAY FROM d.check_date) as check_day,
            yt.id as task_id
        FROM team_members m
        CROSS JOIN date_range d
        LEFT JOIN year_tasks yt ON
            yt.team_member_id = m.id AND
            yt.year = EXTRACT(YEAR FROM d.check_date)::integer
        WHERE m.id IN (13, 14)  -- Временно добавим фильтр для отладки
        ORDER BY m.id, d.check_date`

    // Сначала выведем отладочную информацию
    debugRows, err := db.Query(query)
    if err != nil {
        return 0, fmt.Errorf("error in debug query: %v", err)
    }
    defer debugRows.Close()

    log.Printf("Debug information for birthday checks:")
    for debugRows.Next() {
        var (
            id int
            name string
            birthday time.Time
            checkDate time.Time
            birthMonth, birthDay, checkMonth, checkDay int
            taskID sql.NullInt64
        )
        if err := debugRows.Scan(&id, &name, &birthday, &checkDate, &birthMonth, &birthDay, &checkMonth, &checkDay, &taskID); err != nil {
            log.Printf("Error scanning debug row: %v", err)
            continue
        }
        log.Printf("ID: %d, Name: %s, Birthday: %v, Check Date: %v, Birth Month/Day: %d/%d, Check Month/Day: %d/%d, Task ID: %v",
            id, name, birthday, checkDate, birthMonth, birthDay, checkMonth, checkDay, taskID.Int64)
    }

    // Теперь выполним основной запрос для создания задач
    query = `
        WITH date_range AS (
            SELECT generate_series(
                CURRENT_DATE,
                CURRENT_DATE + INTERVAL '3 days',
                INTERVAL '1 day'
            )::date as check_date
        )
        SELECT DISTINCT
            m.id
        FROM team_members m
        CROSS JOIN date_range d
        LEFT JOIN year_tasks yt ON
            yt.team_member_id = m.id AND
            yt.year = EXTRACT(YEAR FROM d.check_date)::integer
        WHERE 
            EXTRACT(MONTH FROM m.birthday) = EXTRACT(MONTH FROM d.check_date)
            AND EXTRACT(DAY FROM m.birthday) = EXTRACT(DAY FROM d.check_date)
            AND yt.id IS NULL`

    rows, err := db.Query(query)
    if err != nil {
        return 0, fmt.Errorf("error checking upcoming birthdays: %v", err)
    }
    defer rows.Close()

    tasksCreated := 0
    currentYear := time.Now().Year()
    for rows.Next() {
        var memberID int
        if err := rows.Scan(&memberID); err != nil {
            log.Printf("Error scanning member ID: %v", err)
            continue
        }

        // Создаем новую задачу для дня рождения
        result, err := db.Exec(`
            INSERT INTO year_tasks (year, team_member_id)
            VALUES ($1, $2)`,
            currentYear, memberID)
        if err != nil {
            log.Printf("Error creating year task: %v", err)
            continue
        }

        rowsAffected, _ := result.RowsAffected()
        tasksCreated += int(rowsAffected)
    }

    if err = rows.Err(); err != nil {
        return tasksCreated, fmt.Errorf("error iterating over members: %v", err)
    }

    if tasksCreated == 0 {
        return 0, fmt.Errorf("no new tasks created")
    }

    return tasksCreated, nil
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

        // Проверяем и создаем задачи
        count, err := checkUpcomingBirthdaysOnce(db)
        if err != nil && err.Error() != "no new tasks created" {
            log.Printf("Error in scheduled birthday check: %v", err)
        } else if count > 0 {
            log.Printf("Created %d new birthday tasks", count)
        }
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
                        t.name as team_name,
                        m.phone_number
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
                err := rows.Scan(
                        &member.ID,
                        &member.Name,
                        &member.Birthday,
                        &member.TeamID,
                        &member.TeamName,
                        &member.PhoneNumber,
                )
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

func createRequestActionsOnce(db *sql.DB) error {
    // Находим задачи без actions типа request
    query := `
        SELECT yt.id, yt.team_member_id
        FROM year_tasks yt
        LEFT JOIN actions a ON a.task_id = yt.id AND a.type = 'request'
        WHERE a.id IS NULL`

    rows, err := db.Query(query)
    if err != nil {
        return fmt.Errorf("error querying tasks without actions: %v", err)
    }
    defer rows.Close()

    actionsCreated := 0
    for rows.Next() {
        var taskID, birthdayMemberID int
        if err := rows.Scan(&taskID, &birthdayMemberID); err != nil {
            log.Printf("Error scanning task: %v", err)
            continue
        }

        // Получаем всех членов, кроме именинника
        memberRows, err := db.Query(`
            SELECT id
            FROM team_members
            WHERE id != $1`,
            birthdayMemberID)
        if err != nil {
            log.Printf("Error getting team members: %v", err)
            continue
        }
        defer memberRows.Close()

        // Создаем actions для каждого члена команды
        for memberRows.Next() {
            var memberID int
            if err := memberRows.Scan(&memberID); err != nil {
                log.Printf("Error scanning member: %v", err)
                continue
            }

            result, err := db.Exec(`
                INSERT INTO actions (task_id, team_member_id, type)
                VALUES ($1, $2, 'request')`,
                taskID, memberID)
            if err != nil {
                log.Printf("Error creating action for member %d: %v", memberID, err)
                continue
            }

            rowsAffected, _ := result.RowsAffected()
            actionsCreated += int(rowsAffected)
        }
    }

    if err = rows.Err(); err != nil {
        return fmt.Errorf("error iterating over tasks: %v", err)
    }

    if actionsCreated == 0 {
        return fmt.Errorf("no new actions created")
    }

    log.Printf("Successfully created %d new request actions", actionsCreated)
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
        query := `SELECT action_id, task_id, birthday_person_name, team_name, teamlead_phone, teamlead_name, telegram_chat_id FROM member_notifications`

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

                messageText := fmt.Sprintf("Привет! Через 3 дня %s из команды %s празднует день рождения! "+
                        "Переведи, пожалуйста, свой вклад в подарок нашему коллеге по номеру телефона %s, получатель %s.",
                        birthdayName, teamName, teamleadPhone, teamleadName)
                msg := tgbotapi.NewMessage(telegramChatID, messageText)
                msg.ReplyMarkup = keyboard

                // Отправляем сообщение
                sentMessage, err := bot.Send(msg)
                if err != nil {
                        log.Printf("Error sending member notification: %v", err)
                        continue
                }

                if err := createMemberNotificationJournal(db, sentMessage, messageText, keyboard, birthdayName, teamName, teamleadName, teamleadPhone, actionID); err != nil {
                    log.Printf("Error logging message to journal: %v", err)
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
        query := `SELECT task_id, birthday_person_name, telegram_chat_id, notified_teamlead_name FROM teamlead_notifications`

        rows, err := db.Query(query)
        if err != nil {
                log.Printf("Error querying for teamlead notifications: %v", err)
                return
        }
        defer rows.Close()

        for rows.Next() {
                var (
                        taskID              int
                        birthdayName        string
                        telegramChatID      int64
                        notifiedTeamleadName string
                )

                err := rows.Scan(&taskID, &birthdayName, &telegramChatID, &notifiedTeamleadName)
                if err != nil {
                        log.Printf("Error scanning teamlead notification data: %v", err)
                        continue
                }

                messageText := fmt.Sprintf("Привет, %s! %s празднует день рождения через 3 дня! "+
                        "Сейчас тебе начнут поступать переводы ему на подарок! "+
                        "Не забудь запланировать поздравление!", notifiedTeamleadName, birthdayName)
                msg := tgbotapi.NewMessage(telegramChatID, messageText)

                // Отправляем сообщение
                sentMessage, err := bot.Send(msg)
                if err != nil {
                        log.Printf("Error sending teamlead notification: %v", err)
                        continue
                }

                if err := createTeamLeadNotificationJournal(db, sentMessage, messageText, birthdayName, taskID); err != nil {
                    log.Printf("Error logging message to journal: %v", err)
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

func sendBirthdayWishesOnce(db *sql.DB, bot *tgbotapi.BotAPI) error {
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
        return fmt.Errorf("error querying birthday people: %v", err)
    }
    defer rows.Close()

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

        messageText := fmt.Sprintf("С днем рождения, %s! 🎉\nЖелаем успехов, счастья и всего самого наилучшего! 🎂", name)
        msg := tgbotapi.NewMessage(telegramChatID, messageText)

        // Отправляем сообщение
        sentMessage, err := bot.Send(msg)
        if err != nil {
            log.Printf("Error sending birthday wish to %s: %v", name, err)
            continue
        }

        if err := createBirthdayWishJournal(db, sentMessage, messageText, memberID, name, teamID); err != nil {
            log.Printf("Error logging message to journal: %v", err)
        }
    }

    if err = rows.Err(); err != nil {
        return fmt.Errorf("error iterating over birthday people: %v", err)
    }

    return nil
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

func isAdmin(db *sql.DB, chatID int64) (bool, error) {
    var exists bool
    err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM admins WHERE telegram_chat_id = $1)", chatID).Scan(&exists)
    if err != nil {
        return false, err
    }
    return exists, nil
}
