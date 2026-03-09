HTTP-сервис с `GET /health` и анонимными server-side сессиями в Redis через `POST /session`.

> Все настройки проекта берутся **только** из `.env.local`.

```env
APP_HOST=localhost
APP_PORT=8080
APP_USER_SESSION_TTL=60

REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0
```

## Запуск

Запуск одной командой:

```bash
make run
```

Проверка, что сервис поднялся:

```bash
curl http://localhost:8080/health
# ожидается: {"status":"ok"}
```

Создать/обновить сессию:

```bash
curl -i -X POST http://localhost:8080/session
```

## Поведение endpoint-ов

- `GET /health`
  - Всегда `200 OK` и `{"status":"ok"}`.
  - Не создаёт сессию и не обновляет TTL в Redis.
  - Если пришла Cookie `X-Session-Id` валидного формата, возвращает её обратно в `Set-Cookie`.
- `POST /session`
  - Без cookie создаёт новую сессию в Redis (`Hash` по ключу `sid:{sid}` с `created_at` и `updated_at`) и возвращает `201 Created`.
  - С существующей cookie обновляет `updated_at` и TTL, возвращает `200 OK`.
  - С невалидной/просроченной cookie создаёт новую сессию, возвращает `201 Created`.

## Формат сессии в Redis

- Ключ: `sid:{session_id}`
- Тип: `Hash`
- Поля: `created_at`, `updated_at` (RFC3339)
- TTL берётся из `APP_USER_SESSION_TTL`