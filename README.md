# EventHub NoSQL

Сервис для обработки событий с несколькими NoSQL-хранилищами:

- `Redis` хранит пользовательские сессии и кэши.
- `MongoDB` в sharded-конфигурации хранит пользователей и события.
- `Cassandra` хранит реакции и отзывы.
- `Neo4j` строит рекомендации по лайкам.

Все runtime-настройки читаются из `.env.local`.

## API

### Сессии и аутентификация

#### `GET /health`

Проверка доступности gateway.

Ответ:

```json
{
  "status": "ok"
}
```

#### `POST /session`

Создаёт анонимную сессию или продлевает существующую.

- `201 Created` если новая сессия создана
- `200 OK` если существующая валидная сессия продлена
- cookie `X-Session-Id` выставляется в `HttpOnly`

#### `POST /users`

Создаёт пользователя и сразу привязывает новую сессию к нему.

Тело:

```json
{
  "full_name": "Alice Example",
  "username": "alice",
  "password": "secret"
}
```

Ответы:

- `201 Created` и cookie `X-Session-Id`
- `409 Conflict` с `{ "message": "user already exists" }`
- `400 Bad Request` при невалидных полях

#### `POST /auth/login`

Логин по `username/password`. Если у клиента уже есть валидная анонимная сессия, сервис привяжет её к пользователю.

Тело:

```json
{
  "username": "alice",
  "password": "secret"
}
```

Ответы:

- `204 No Content` и cookie `X-Session-Id`
- `401 Unauthorized` с `{ "message": "invalid credentials" }`

#### `POST /auth/logout`

Требует авторизованную сессию.

Ответы:

- `204 No Content` и cookie с `Max-Age=0`
- `401 Unauthorized` если пользователь не авторизован

### Пользователи

#### `GET /users`

Список пользователей.

Query-параметры:

- `id`
- `name`
- `limit`
- `offset`

Ответ:

```json
{
  "users": [
    {
      "id": "6838a509c88c466b1142d79f",
      "full_name": "Alice Example",
      "username": "alice"
    }
  ],
  "count": 1
}
```

#### `GET /users/{id}`

Получение пользователя по идентификатору.

Ответы:

- `200 OK` с объектом пользователя
- `404 Not Found` с `{ "message": "Not found" }`

#### `GET /users/{id}/events`

Список событий, созданных пользователем.

Поддерживает те же фильтры, что и `GET /events`.

Ответы:

- `200 OK`
- `404 Not Found` с `{ "message": "User not found" }`

### События

#### `POST /events`

Создание события. Требует авторизованную сессию.

Тело:

```json
{
  "title": "NoSQL Meetup",
  "address": "Kronverkskiy prospekt 49",
  "started_at": "2030-01-15T18:00:00Z",
  "finished_at": "2030-01-15T21:00:00Z",
  "description": "Talks and networking"
}
```

Ответ:

```json
{
  "id": "6838a60123ad784ab518de0b"
}
```

Ошибки:

- `401 Unauthorized`
- `409 Conflict` с `{ "message": "event already exists" }`
- `400 Bad Request` при невалидных полях

#### `PATCH /events/{id}`

Частичное обновление события. Изменять событие может только его организатор.

Допустимые поля:

```json
{
  "category": "meetup",
  "price": 1500,
  "city": "Saint Petersburg"
}
```

Ограничения:

- `category` только из списка: `meetup`, `concert`, `exhibition`, `party`, `other`
- `price` целое число `>= 0`
- `city` можно передать пустой строкой, чтобы удалить поле из документа

Ответы:

- `204 No Content`
- `404 Not Found` с `{ "message": "Not found. Be sure that event exists and you are the organizer" }`
- `400 Bad Request`

#### `POST /events/{id}/like`

Ставит лайк событию. Требует авторизованную сессию.

Ответы:

- `204 No Content`
- `401 Unauthorized`
- `404 Not Found` с `{ "message": "Event not found" }`

#### `POST /events/{id}/dislike`

Ставит дизлайк событию. Требует авторизованную сессию.

Ответы такие же, как у лайка.

#### `GET /events/{id}`

Получение события по идентификатору.

Поддерживает query-параметр `include`:

- `include=reactions`
- `include=reviews`
- `include=reactions,reviews`

Пример ответа:

```json
{
  "id": "6838a60123ad784ab518de0b",
  "title": "NoSQL Meetup",
  "category": "meetup",
  "price": 1500,
  "description": "Talks and networking",
  "location": {
    "city": "Saint Petersburg",
    "address": "Kronverkskiy prospekt 49"
  },
  "created_at": "2026-05-29T16:35:18.925285300Z",
  "created_by": "6838a509c88c466b1142d79f",
  "started_at": "2030-01-15T18:00:00Z",
  "finished_at": "2030-01-15T21:00:00Z",
  "reactions": {
    "likes": 1,
    "dislikes": 0
  },
  "reviews": {
    "count": 1,
    "rating": 5.0
  }
}
```

#### `GET /events`

Список событий с фильтрацией.

Query-параметры:

- `id`
- `title`
- `category`
- `city`
- `user_id`
- `user` — точное совпадение по `username` автора
- `address` — частичное совпадение по адресу
- `price_from`
- `price_to`
- `date_from` и `date_to` в формате `YYYYMMDD`
- `started_date_from` и `started_date_to` — алиасы для тех же фильтров
- `limit`
- `offset`
- `include=reactions`
- `include=reviews`
- `include=reactions,reviews`

Ответ:

```json
{
  "events": [],
  "count": 0
}
```

### Отзывы

#### `POST /events/{event_id}/reviews`

Создаёт отзыв. Требует авторизованную сессию.

Тело:

```json
{
  "comment": "Well organised event",
  "rating": 5
}
```

Ограничения:

- `comment` обязателен, длина `1..300`
- `rating` обязателен, диапазон `1..5`
- один пользователь может оставить только один отзыв на одно событие

Ответы:

- `201 Created` с `{ "id": "<uuid>" }`
- `404 Not Found` с `{ "message": "Event not found" }`
- `409 Conflict` с `{ "message": "Already exists" }`

#### `GET /events/{event_id}/reviews`

Список отзывов по событию.

Query-параметры:

- `limit`
- `offset`

Ответ:

```json
{
  "reviews": [
    {
      "id": "f5a08ebf-f3cb-4f88-9739-18f2950bff16",
      "event_id": "6838a60123ad784ab518de0b",
      "comment": "Well organised event",
      "created_at": "2026-05-29T16:37:15.086Z",
      "created_by": "6838a509c88c466b1142d79f",
      "rating": 5,
      "updated_at": "2026-05-29T16:37:15.086Z"
    }
  ],
  "count": 1
}
```

#### `PATCH /events/{event_id}/reviews/{review_id}`

Частично обновляет отзыв. Менять отзыв может только его автор.

Тело может содержать любое сочетание полей:

```json
{
  "comment": "Updated comment",
  "rating": 4
}
```

Ответы:

- `204 No Content`
- `404 Not Found` с `{ "message": "Event not found" }`
- `400 Bad Request`

### Рекомендации

#### `GET /recommendations`

Возвращает персональные рекомендации. Требует авторизованную сессию.

Ответ:

```json
{
  "events": [],
  "count": 0
}
```

Список формируется по лайкам в `Neo4j` и кэшируется в `Redis`.

## Cookie и сессии

- gateway использует `HttpOnly` cookie `X-Session-Id`
- сессия хранится в Redis как `sid:{session_id}`
- в hash лежат поля `created_at`, `updated_at`, `user_id`
- TTL задаётся через `APP_USER_SESSION_TTL`

## Хранилища и модель данных

### Redis

- `sid:{session_id}` — пользовательские сессии
- `events:{md5(title)}:reactions` и `event:{md5(title)}:reactions` — кэш лайков/дизлайков
- `event:{md5(title)}:reviews` — кэш summary по отзывам
- `user:{user_id}:recomms` — кэш рекомендаций

### MongoDB

- коллекция `users`
- коллекция `events`
- `events` шардируется по `created_by` через hashed key

### Cassandra

- `${CASSANDRA_KEYSPACE}.event_reactions`
- `${CASSANDRA_KEYSPACE}.event_reviews`

### Neo4j

- узлы `User` и `Event`
- связь `LIKED`
- рекомендации строятся по collaborative filtering на основе лайков