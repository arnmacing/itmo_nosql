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
MONGODB_HOST=mongos
MONGODB_PORT=mongos_port

MONGO_CFG_PORT=configsvr_port
MONGO_SHARD1A_PORT=shard1a_port
MONGO_SHARD1B_PORT=shard1b_port
MONGO_SHARD1C_PORT=shard1c_port
MONGO_SHARD2A_PORT=shard2a_port
MONGO_SHARD2B_PORT=shard2b_port
MONGO_SHARD2C_PORT=shard2c_port
```

## Сервисы

- `gateway` – публичный API.
- `session-service` – сессии в Redis.
- `user-service` – пользователи и аутентификация.
- `event-service` – события и фильтрация.
- `redis` – хранение сессий.
- `cfg1`, `shard1*`, `shard2*`, `mongos`, `mongo-init` – MongoDB sharded cluster.

## Запуск

```bash
make run
```

Публичная точка входа: `http://localhost:${APP_PORT}`.

## Публичные endpoint-ы

- `GET /health`
- `POST /session`
- `POST /users`
- `GET /users`
- `GET /users/{id}`
- `GET /users/{id}/events`
- `POST /auth/login`
- `POST /auth/logout`
- `POST /events`
- `PATCH /events/{id}`
- `GET /events`
- `GET /events/{id}`

## Redis

- Ключ: `sid:{session_id}`
- Тип: `Hash`
- Поля: `created_at`, `updated_at`, `user_id`
- TTL: `APP_USER_SESSION_TTL`