package healthcheck;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import java.security.SecureRandom;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String PORT_ENV = "APP_PORT";
    private static final String SESSION_TTL_ENV = "APP_USER_SESSION_TTL";
    private static final String REDIS_HOST_ENV = "REDIS_HOST";
    private static final String REDIS_PORT_ENV = "REDIS_PORT";
    private static final String REDIS_PASSWORD_ENV = "REDIS_PASSWORD";
    private static final String REDIS_DB_ENV = "REDIS_DB";
    private static final String MONGODB_DATABASE_ENV = "MONGODB_DATABASE";
    private static final String MONGODB_DATABASE_FALLBACK_ENV = "MONGODB_DATABASE";
    private static final String MONGODB_USER_ENV = "MONGODB_USER";
    private static final String MONGODB_PASSWORD_ENV = "MONGODB_PASSWORD";
    private static final String MONGODB_HOST_ENV = "MONGODB_HOST";
    private static final String MONGODB_PORT_ENV = "MONGODB_PORT";
    private static final String SESSION_COOKIE_NAME = "X-Session-Id";
    private static final Pattern SID_PATTERN = Pattern.compile("^[0-9a-f]{32}$");
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String CREATE_SESSION_SCRIPT = """
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'created_at', ARGV[1], 'updated_at', ARGV[1])
            if ARGV[3] ~= '' then
                redis.call('HSET', KEYS[1], 'user_id', ARGV[3])
            end
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return 1
            """;

    private static final String TOUCH_SESSION_SCRIPT = """
            if redis.call('EXISTS', KEYS[1]) == 0 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'updated_at', ARGV[1])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return 1
            """;

    private static final String BIND_SESSION_TO_USER_SCRIPT = """
            if redis.call('EXISTS', KEYS[1]) == 0 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'user_id', ARGV[1], 'updated_at', ARGV[2])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
            return 1
            """;

    private record HealthResponse(String status) {
    }

    private record MessageResponse(String message) {}

    private record EventIdResponse(String id) {}

    private record EventsListResponse(List<EventResponse> events, int count) {}

    private record EventResponse(
            String id,
            String title,
            String description,
            EventLocation location,
            String created_at,
            String created_by,
            String started_at,
            String finished_at
    ) {}

    private record EventLocation(String address) {}

    private record UserCreateRequest(String full_name, String username, String password) {}

    private record LoginRequest(String username, String password) {}

    private record EventCreateRequest(
            String title,
            String address,
            String started_at,
            String finished_at,
            String description
    ) {}

    private record SessionInfo(boolean exists, String userId) {
        private static SessionInfo missing() {
            return new SessionInfo(false, null);
        }

        private static SessionInfo present(String userId) {
            return new SessionInfo(true, userId);
        }
    }

    public static void main(String[] args) {
        int port = requirePort(PORT_ENV);
        int sessionTtlSeconds = requirePositiveInt(SESSION_TTL_ENV);

        String redisHost = requireNonBlank(REDIS_HOST_ENV);
        int redisPort = requirePort(REDIS_PORT_ENV);
        int redisDb = requireNonNegativeInt(REDIS_DB_ENV);
        String redisPassword = trimToEmpty(System.getenv(REDIS_PASSWORD_ENV));

        String mongoDatabase = requireMongoDatabaseName();
        String mongoUser = trimToEmpty(System.getenv(MONGODB_USER_ENV));
        String mongoPassword = trimToEmpty(System.getenv(MONGODB_PASSWORD_ENV));
        String mongoHost = requireNonBlank(MONGODB_HOST_ENV);
        int mongoPort = requirePort(MONGODB_PORT_ENV);

        SessionStore sessionStore = new SessionStore(redisHost, redisPort, redisPassword, redisDb, sessionTtlSeconds);
        MongoStore mongoStore = new MongoStore(mongoHost, mongoPort, mongoDatabase, mongoUser, mongoPassword);

        Javalin app = Javalin.create();

        app.get("/health", ctx -> {
            String sid = readValidSidFromCookie(ctx);
            maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);
            ctx.json(new HealthResponse("ok"));
        });

        app.post("/session", ctx -> handleCreateOrRefreshSession(ctx, sessionStore, sessionTtlSeconds));
        app.post("/users", ctx -> handleCreateUser(ctx, sessionStore, mongoStore, sessionTtlSeconds));
        app.post("/auth/login", ctx -> handleLogin(ctx, sessionStore, mongoStore, sessionTtlSeconds));
        app.post("/auth/logout", ctx -> handleLogout(ctx, sessionStore));
        app.post("/events", ctx -> handleCreateEvent(ctx, sessionStore, mongoStore, sessionTtlSeconds));
        app.get("/events", ctx -> handleListEvents(ctx, mongoStore, sessionTtlSeconds));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            sessionStore.close();
            mongoStore.close();
        }));

        app.start(port);
        log.info("Service started on port {}", port);
    }

    private static void handleCreateOrRefreshSession(Context ctx, SessionStore sessionStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        if (sid == null) {
            String newSid = sessionStore.createAnonymousSession();
            setSessionCookie(ctx, newSid, sessionTtlSeconds);
            ctx.status(201);
            return;
        }

        if (sessionStore.touchSession(sid)) {
            setSessionCookie(ctx, sid, sessionTtlSeconds);
            ctx.status(200);
            return;
        }

        String newSid = sessionStore.createAnonymousSession();
        setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(201);
    }

    private static void handleCreateUser(Context ctx, SessionStore sessionStore, MongoStore mongoStore, int sessionTtlSeconds) {
        String requestSid = readValidSidFromCookie(ctx);

        UserCreateRequest request = readBody(ctx, UserCreateRequest.class);
        if (request == null) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("full_name"));
            return;
        }

        if (isBlank(request.full_name())) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("full_name"));
            return;
        }

        if (isBlank(request.username())) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("username"));
            return;
        }

        if (isBlank(request.password())) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("password"));
            return;
        }

        UserCreationResult result = mongoStore.createUser(request.full_name().trim(), request.username().trim(), request.password());
        if (!result.created()) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(409).json(new MessageResponse("user already exists"));
            return;
        }

        String newSid = sessionStore.createUserSession(result.userId());
        setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(201);
    }

    private static void handleLogin(Context ctx, SessionStore sessionStore, MongoStore mongoStore, int sessionTtlSeconds) {
        String requestSid = readValidSidFromCookie(ctx);

        LoginRequest request = readBody(ctx, LoginRequest.class);
        if (request == null) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("username"));
            return;
        }

        if (isBlank(request.username())) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("username"));
            return;
        }

        if (isBlank(request.password())) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(invalidFieldMessage("password"));
            return;
        }

        String userId = mongoStore.findUserIdByCredentials(request.username().trim(), request.password());
        if (userId == null) {
            touchSessionIfExists(sessionStore, requestSid);
            maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(401).json(new MessageResponse("invalid credentials"));
            return;
        }

        if (requestSid != null && sessionStore.bindSessionToUser(requestSid, userId)) {
            setSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(204);
            return;
        }

        String newSid = sessionStore.createUserSession(userId);
        setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(204);
    }

    private static void handleLogout(Context ctx, SessionStore sessionStore) {
        String requestSid = readValidSidFromCookie(ctx);
        if (requestSid != null) {
            sessionStore.deleteSession(requestSid);
        }

        clearSessionCookie(ctx, requestSid);
        ctx.status(204);
    }

    private static void handleCreateEvent(Context ctx, SessionStore sessionStore, MongoStore mongoStore, int sessionTtlSeconds) {
        String requestSid = readValidSidFromCookie(ctx);
        if (requestSid == null) {
            ctx.status(401);
            return;
        }

        maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);

        SessionInfo sessionInfo = sessionStore.readSession(requestSid);
        if (!sessionInfo.exists()) {
            ctx.status(401);
            return;
        }

        touchSessionIfExists(sessionStore, requestSid);

        if (isBlank(sessionInfo.userId())) {
            ctx.status(401);
            return;
        }

        EventCreateRequest request = readBody(ctx, EventCreateRequest.class);
        if (request == null) {
            ctx.status(400).json(invalidFieldMessage("title"));
            return;
        }

        if (isBlank(request.title())) {
            ctx.status(400).json(invalidFieldMessage("title"));
            return;
        }

        if (isBlank(request.address())) {
            ctx.status(400).json(invalidFieldMessage("address"));
            return;
        }

        if (isBlank(request.started_at()) || !isValidRfc3339(request.started_at())) {
            ctx.status(400).json(invalidFieldMessage("started_at"));
            return;
        }

        if (isBlank(request.finished_at()) || !isValidRfc3339(request.finished_at())) {
            ctx.status(400).json(invalidFieldMessage("finished_at"));
            return;
        }

        EventCreationResult result = mongoStore.createEvent(
                request.title().trim(),
                defaultString(request.description()),
                request.address().trim(),
                request.started_at(),
                request.finished_at(),
                sessionInfo.userId()
        );

        if (!result.created()) {
            ctx.status(409).json(new MessageResponse("event already exists"));
            return;
        }

        ctx.status(201).json(new EventIdResponse(result.eventId()));
    }

    private static void handleListEvents(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        Integer limit;
        try {
            limit = parseUnsignedQueryInt(ctx.queryParam("limit"));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidParameterMessage("limit"));
            return;
        }

        Integer offsetRaw;
        try {
            offsetRaw = parseUnsignedQueryInt(ctx.queryParam("offset"));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidParameterMessage("offset"));
            return;
        }

        int offset = offsetRaw == null ? 0 : offsetRaw;

        String titleFilter = ctx.queryParam("title");
        List<EventResponse> events = mongoStore.listEvents(titleFilter, limit, offset);
        ctx.json(new EventsListResponse(events, events.size()));
    }

    private static void maybeSetSessionCookie(Context ctx, String sid, int ttlSeconds) {
        if (sid != null) {
            setSessionCookie(ctx, sid, ttlSeconds);
        }
    }

    private static void setSessionCookie(Context ctx, String sid, int ttlSeconds) {
        ctx.header("Set-Cookie", SESSION_COOKIE_NAME + "=" + sid + "; HttpOnly; Path=/; Max-Age=" + ttlSeconds);
    }

    private static void clearSessionCookie(Context ctx, String sid) {
        String cookieValue = sid == null ? "" : sid;
        ctx.header("Set-Cookie", SESSION_COOKIE_NAME + "=" + cookieValue + "; HttpOnly; Path=/; Max-Age=0");
    }

    private static void touchSessionIfExists(SessionStore sessionStore, String sid) {
        if (sid != null) {
            sessionStore.touchSession(sid);
        }
    }

    private static MessageResponse invalidFieldMessage(String field) {
        return new MessageResponse("invalid \"" + field + "\" field");
    }

    private static MessageResponse invalidParameterMessage(String field) {
        return new MessageResponse("invalid \"" + field + "\" parameter");
    }

    private static boolean isValidRfc3339(String value) {
        try {
            OffsetDateTime.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static Integer parseUnsignedQueryInt(String value) {
        if (value == null) {
            return null;
        }

        long parsed = Long.parseLong(value.trim());
        if (parsed < 0 || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid query integer");
        }

        return (int) parsed;
    }

    private static String readValidSidFromCookie(Context ctx) {
        String rawSid = ctx.cookie(SESSION_COOKIE_NAME);
        if (rawSid == null) {
            return null;
        }

        String sid = rawSid.trim();
        return SID_PATTERN.matcher(sid).matches() ? sid : null;
    }

    private static String generateSid() {
        byte[] bytes = new byte[16];
        SECURE_RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static <T> T readBody(Context ctx, Class<T> clazz) {
        try {
            return ctx.bodyAsClass(clazz);
        } catch (Exception e) {
            return null;
        }
    }

    private static String requireMongoDatabaseName() {
        String raw = System.getenv(MONGODB_DATABASE_ENV);
        if (raw != null && !raw.isBlank()) {
            return raw.trim();
        }

        String fallback = System.getenv(MONGODB_DATABASE_FALLBACK_ENV);
        if (fallback != null && !fallback.isBlank()) {
            return fallback.trim();
        }

        fail("Environment variable {} is required.", MONGODB_DATABASE_ENV);
        return "";
    }

    private static int requirePositiveInt(String name) {
        String raw = requireNonBlank(name);
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 1) {
                fail("Invalid {}={}", name, raw);
            }
            return value;
        } catch (NumberFormatException e) {
            fail("Invalid {}={}", name, raw);
            return -1;
        }
    }

    private static int requireNonNegativeInt(String name) {
        String raw = requireNonBlank(name);
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 0) {
                fail("Invalid {}={}", name, raw);
            }
            return value;
        } catch (NumberFormatException e) {
            fail("Invalid {}={}", name, raw);
            return -1;
        }
    }

    private static String requireNonBlank(String name) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            fail("Environment variable {} is required.", name);
        }
        return raw.trim();
    }

    private static int requirePort(String name) {
        String raw = requireNonBlank(name);

        try {
            int port = Integer.parseInt(raw.trim());
            if (port < 1 || port > 65535) {
                fail("Invalid {}={}", name, raw);
            }
            return port;
        } catch (NumberFormatException e) {
            fail("Invalid {}={}", name, raw);
            return -1;
        }
    }

    private static void fail(String message, Object... args) {
        log.error(message, args);
        System.exit(1);
    }

    private record UserCreationResult(boolean created, String userId) {
        private static UserCreationResult created(String userId) {
            return new UserCreationResult(true, userId);
        }

        private static UserCreationResult conflict() {
            return new UserCreationResult(false, null);
        }
    }

    private record EventCreationResult(boolean created, String eventId) {
        private static EventCreationResult created(String eventId) {
            return new EventCreationResult(true, eventId);
        }

        private static EventCreationResult conflict() {
            return new EventCreationResult(false, null);
        }
    }

    private static final class SessionStore implements AutoCloseable {

        private final JedisPooled jedis;
        private final int sessionTtlSeconds;

        private SessionStore(String redisHost, int redisPort, String redisPassword, int redisDb, int sessionTtlSeconds) {
            DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                    .database(redisDb);

            if (!redisPassword.isBlank()) {
                configBuilder.password(redisPassword);
            }

            this.jedis = new JedisPooled(new HostAndPort(redisHost, redisPort), configBuilder.build());
            this.sessionTtlSeconds = sessionTtlSeconds;
        }

        private String createAnonymousSession() {
            return createSessionWithOptionalUser("");
        }

        private String createUserSession(String userId) {
            return createSessionWithOptionalUser(userId);
        }

        private String createSessionWithOptionalUser(String userId) {
            for (int attempt = 0; attempt < 10; attempt++) {
                String sid = generateSid();
                String now = Instant.now().toString();
                Object result = jedis.eval(
                        CREATE_SESSION_SCRIPT,
                        List.of(redisKey(sid)),
                        List.of(now, String.valueOf(sessionTtlSeconds), userId == null ? "" : userId)
                );
                if (asLong(result) == 1L) {
                    return sid;
                }
            }

            throw new IllegalStateException("Failed to create session");
        }

        private boolean touchSession(String sid) {
            String now = Instant.now().toString();
            Object result = jedis.eval(
                    TOUCH_SESSION_SCRIPT,
                    List.of(redisKey(sid)),
                    List.of(now, String.valueOf(sessionTtlSeconds))
            );
            return asLong(result) == 1L;
        }

        private boolean bindSessionToUser(String sid, String userId) {
            String now = Instant.now().toString();
            Object result = jedis.eval(
                    BIND_SESSION_TO_USER_SCRIPT,
                    List.of(redisKey(sid)),
                    List.of(userId, now, String.valueOf(sessionTtlSeconds))
            );
            return asLong(result) == 1L;
        }

        private SessionInfo readSession(String sid) {
            Map<String, String> values = jedis.hgetAll(redisKey(sid));
            if (values == null || values.isEmpty()) {
                return SessionInfo.missing();
            }
            return SessionInfo.present(values.get("user_id"));
        }

        private void deleteSession(String sid) {
            jedis.del(redisKey(sid));
        }

        private static String redisKey(String sid) {
            return "sid:" + sid;
        }

        private static long asLong(Object value) {
            if (value instanceof Number number) {
                return number.longValue();
            }
            return Long.parseLong(String.valueOf(value));
        }

        @Override
        public void close() {
            jedis.close();
        }
    }

    private static final class MongoStore implements AutoCloseable {

        private final MongoClient mongoClient;
        private final MongoCollection<Document> usersCollection;
        private final MongoCollection<Document> eventsCollection;

        private MongoStore(String host, int port, String databaseName, String username, String password) {
            this.mongoClient = createMongoClient(host, port, databaseName, username, password);
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            this.usersCollection = database.getCollection("users");
            this.eventsCollection = database.getCollection("events");
            ensureIndexes();
        }

        private static MongoClient createMongoClient(String host, int port, String databaseName, String username, String password) {
            if (username.isBlank()) {
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyToClusterSettings(builder -> builder.hosts(List.of(new ServerAddress(host, port))))
                        .build();
                return MongoClients.create(settings);
            }

            MongoClient clientWithDatabaseAuth = MongoClients.create(buildSettings(host, port,
                    MongoCredential.createCredential(username, databaseName, password.toCharArray())));
            if (ping(clientWithDatabaseAuth, databaseName)) {
                return clientWithDatabaseAuth;
            }
            clientWithDatabaseAuth.close();

            MongoClient clientWithAdminAuth = MongoClients.create(buildSettings(host, port,
                    MongoCredential.createCredential(username, "admin", password.toCharArray())));
            if (ping(clientWithAdminAuth, databaseName)) {
                return clientWithAdminAuth;
            }
            clientWithAdminAuth.close();
            throw new IllegalStateException("Failed to authenticate to MongoDB using provided credentials.");
        }

        private static MongoClientSettings buildSettings(String host, int port, MongoCredential credential) {
            return MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(List.of(new ServerAddress(host, port))))
                    .credential(credential)
                    .build();
        }

        private static boolean ping(MongoClient client, String databaseName) {
            try {
                client.getDatabase(databaseName).runCommand(new Document("ping", 1));
                return true;
            } catch (MongoException e) {
                return false;
            }
        }

        private void ensureIndexes() {
            usersCollection.createIndex(Indexes.ascending("username"), new IndexOptions().unique(true));
            eventsCollection.createIndex(Indexes.ascending("title"), new IndexOptions().unique(true));
            eventsCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("title"), Indexes.ascending("created_by")));
            eventsCollection.createIndex(Indexes.ascending("created_by"));
        }

        private UserCreationResult createUser(String fullName, String username, String password) {
            String passwordHash = BCrypt.hashpw(password, BCrypt.gensalt());

            Document document = new Document("full_name", fullName)
                    .append("username", username)
                    .append("password_hash", passwordHash);

            try {
                usersCollection.insertOne(document);
                ObjectId id = document.getObjectId("_id");
                return UserCreationResult.created(id.toHexString());
            } catch (MongoWriteException e) {
                if (isDuplicateKey(e)) {
                    return UserCreationResult.conflict();
                }
                throw e;
            }
        }

        private String findUserIdByCredentials(String username, String password) {
            Document user = usersCollection.find(Filters.eq("username", username)).first();
            if (user == null) {
                return null;
            }

            String hash = user.getString("password_hash");
            if (isBlank(hash)) {
                return null;
            }

            boolean matches;
            try {
                matches = BCrypt.checkpw(password, hash);
            } catch (IllegalArgumentException e) {
                return null;
            }

            if (!matches) {
                return null;
            }

            ObjectId userId = user.getObjectId("_id");
            return userId == null ? null : userId.toHexString();
        }

        private EventCreationResult createEvent(
                String title,
                String description,
                String address,
                String startedAt,
                String finishedAt,
                String userId
        ) {
            Document event = new Document("title", title)
                    .append("description", description)
                    .append("location", new Document("address", address))
                    .append("created_at", OffsetDateTime.now().toString())
                    .append("created_by", userId)
                    .append("started_at", startedAt)
                    .append("finished_at", finishedAt);

            try {
                eventsCollection.insertOne(event);
                ObjectId id = event.getObjectId("_id");
                return EventCreationResult.created(id.toHexString());
            } catch (MongoWriteException e) {
                if (isDuplicateKey(e)) {
                    return EventCreationResult.conflict();
                }
                throw e;
            }
        }

        private List<EventResponse> listEvents(String titleFilter, Integer limit, int offset) {
            Bson filter = new Document();
            if (!isBlank(titleFilter)) {
                filter = Filters.regex("title", Pattern.compile(Pattern.quote(titleFilter.trim()), Pattern.CASE_INSENSITIVE));
            }

            FindIterable<Document> cursor = eventsCollection.find(filter);
            if (offset > 0) {
                cursor = cursor.skip(offset);
            }

            if (limit != null) {
                cursor = cursor.limit(limit);
            }

            List<EventResponse> events = new ArrayList<>();
            for (Document document : cursor) {
                ObjectId id = document.getObjectId("_id");
                Document locationDocument = document.get("location", Document.class);
                String address = locationDocument == null ? "" : defaultString(locationDocument.getString("address"));

                events.add(new EventResponse(
                        id == null ? "" : id.toHexString(),
                        defaultString(document.getString("title")),
                        defaultString(document.getString("description")),
                        new EventLocation(address),
                        defaultString(document.getString("created_at")),
                        defaultString(document.getString("created_by")),
                        defaultString(document.getString("started_at")),
                        defaultString(document.getString("finished_at"))
                ));
            }
            return events;
        }

        private static boolean isDuplicateKey(MongoWriteException e) {
            return e.getError() != null && e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY;
        }

        @Override
        public void close() {
            mongoClient.close();
        }
    }
}