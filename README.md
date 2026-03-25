HTTP-сервис с Redis-сессиями и MongoDB для пользователей и событий.

> Все настройки проекта берутся **только** из `.env.local`.

```env
APP_HOST=localhost
APP_PORT=8080
APP_USER_SESSION_TTL=60

REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

MONGODB_DATABSE=eventhub
MONGODB_DATABASE=eventhub
MONGODB_USER=eventhub
MONGODB_PASSWORD=eventhub
MONGODB_HOST=mongodb
MONGODB_PORT=27017
```

## Запуск

```bash
make run
```

## Endpoint-ы

- `GET /health` – healthcheck, без изменений состояния в Redis.
- `POST /session` – создание/обновление анонимной сессии.
- `POST /users` – регистрация пользователя, создание новой сессии с `user_id`.
- `POST /auth/login` – вход.
- `POST /auth/logout` – выход, удаление сессии и cookie.
- `POST /events` – создание события (только авторизованный пользователь).
- `GET /events` – просмотр событий.

## MongoDB коллекции и индексы

- `users`
    - поля: `full_name`, `username`, `password_hash`
    - индекс: `username` unique
- `events`
    - поля: `title`, `description`, `location.address`, `created_at`, `created_by`, `started_at`, `finished_at`
    - индексы: `title` unique, `(title, created_by)`, `created_by`

## Redis сессии

- Ключ: `sid:{session_id}`
- Тип: `Hash`
- Поля: `created_at`, `updated_at`, `user_id`
- TTL: `APP_USER_SESSION_TTL`