HTTP-сервис с Redis-сессиями и MongoDB для пользователей и событий.

> Все настройки проекта берутся **только** из `.env.local`.

```env
APP_HOST=localhost
APP_PORT=app_port
APP_USER_SESSION_TTL=60
SESSION_SERVICE_PORT=session_service_port
USER_SERVICE_PORT=user_service_port
EVENT_SERVICE_PORT=event_service_port

REDIS_HOST=redis
REDIS_PORT=redis_port
REDIS_PASSWORD=
REDIS_DB=0

MONGODB_DATABASE=eventhub
MONGODB_USER=eventhub
MONGODB_PASSWORD=eventhub
MONGODB_HOST=mongodb
MONGODB_PORT=mongodb_port
```

## Сервисы

- `gateway` – публичный API (`/health`, `/session`, `/users`, `/auth/*`, `/events`).
- `session-service` – Redis-сессии (`/internal/sessions/*`).
- `user-service` – регистрация и аутентификация пользователей (`/internal/users`, `/internal/auth/login`).
- `event-service` – создание и выдача событий (`/internal/events`).
- `redis`, `mongodb` – инфраструктура хранения.

## Запуск

```bash
make run
```

Публичная точка входа: `http://localhost:${APP_PORT}`.

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