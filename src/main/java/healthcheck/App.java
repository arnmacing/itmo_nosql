package healthcheck;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FILTER_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE
            .withResolverStyle(ResolverStyle.STRICT);
    private static final Set<String> VALID_EVENT_CATEGORIES = Set.of(
            "meetup",
            "concert",
            "exhibition",
            "party",
            "other"
    );

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
            String category,
            Integer price,
            String description,
            EventLocation location,
            String created_at,
            String created_by,
            String started_at,
            String finished_at
    ) {}

    private record EventLocation(String city, String address) {}

    private record UsersListResponse(List<UserResponse> users, int count) {}

    private record UserResponse(String id, String full_name, String username) {}

    private record UserCreateRequest(String full_name, String username, String password) {}

    private record LoginRequest(String username, String password) {}

    private record EventCreateRequest(
            String title,
            String address,
            String started_at,
            String finished_at,
            String description
    ) {}

    private record EventPatchRequest(
            boolean hasCategory,
            String category,
            boolean hasPrice,
            Integer price,
            boolean hasCity,
            String city
    ) {}

    private record EventFilters(
            String id,
            String title,
            String category,
            Integer priceFrom,
            Integer priceTo,
            String city,
            LocalDate dateFrom,
            LocalDate dateTo,
            String userId,
            String username,
            String address,
            Integer limit,
            int offset
    ) {}

    private record UserFilters(
            String id,
            String name,
            Integer limit,
            int offset
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
        app.patch("/events/{id}", ctx -> handlePatchEvent(ctx, sessionStore, mongoStore, sessionTtlSeconds));
        app.get("/events/{id}", ctx -> handleGetEvent(ctx, mongoStore, sessionTtlSeconds));
        app.get("/events", ctx -> handleListEvents(ctx, mongoStore, sessionTtlSeconds));
        app.get("/users/{id}/events", ctx -> handleListUserEvents(ctx, mongoStore, sessionTtlSeconds));
        app.get("/users/{id}", ctx -> handleGetUser(ctx, mongoStore, sessionTtlSeconds));
        app.get("/users", ctx -> handleListUsers(ctx, mongoStore, sessionTtlSeconds));

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
        if (requestSid == null) {
            ctx.status(401);
            return;
        }

        SessionInfo sessionInfo = sessionStore.readSession(requestSid);
        if (!sessionInfo.exists() || isBlank(sessionInfo.userId())) {
            ctx.status(401);
            return;
        }

        sessionStore.deleteSession(requestSid);
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

    private static void handlePatchEvent(Context ctx, SessionStore sessionStore, MongoStore mongoStore, int sessionTtlSeconds) {
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

        EventPatchRequest patchRequest;
        try {
            patchRequest = parseEventPatchRequest(ctx);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidFieldMessage(e.getMessage()));
            return;
        }

        String eventId = trimToEmpty(ctx.pathParam("id"));
        if (isBlank(eventId)) {
            ctx.status(404).json(new MessageResponse("Not found. Be sure that event exists and you are the organizer"));
            return;
        }

        boolean updated = mongoStore.updateEventByOrganizer(eventId, sessionInfo.userId(), patchRequest);
        if (!updated) {
            ctx.status(404).json(new MessageResponse("Not found. Be sure that event exists and you are the organizer"));
            return;
        }

        ctx.status(204);
    }

    private static void handleGetEvent(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String eventId = trimToEmpty(ctx.pathParam("id"));
        EventResponse event = mongoStore.getEventById(eventId);
        if (event == null) {
            ctx.status(404).json(new MessageResponse("Not found"));
            return;
        }

        ctx.json(event);
    }

    private static void handleListEvents(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        EventFilters filters;
        try {
            filters = parseEventFilters(ctx);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidFieldMessage(e.getMessage()));
            return;
        }

        List<EventResponse> events = mongoStore.listEvents(filters);
        ctx.json(new EventsListResponse(events, events.size()));
    }

    private static void handleListUsers(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        UserFilters filters;
        try {
            filters = parseUserFilters(ctx);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidFieldMessage(e.getMessage()));
            return;
        }

        List<UserResponse> users = mongoStore.listUsers(filters);
        ctx.json(new UsersListResponse(users, users.size()));
    }

    private static void handleGetUser(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String userId = trimToEmpty(ctx.pathParam("id"));
        UserResponse user = mongoStore.getUserById(userId);
        if (user == null) {
            ctx.status(404).json(new MessageResponse("Not found"));
            return;
        }

        ctx.json(user);
    }

    private static void handleListUserEvents(Context ctx, MongoStore mongoStore, int sessionTtlSeconds) {
        String sid = readValidSidFromCookie(ctx);
        maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String userId = trimToEmpty(ctx.pathParam("id"));
        if (!mongoStore.userExists(userId)) {
            ctx.status(404).json(new MessageResponse("User not found"));
            return;
        }

        EventFilters filters;
        try {
            EventFilters baseFilters = parseEventFilters(ctx);
            filters = new EventFilters(
                    baseFilters.id(),
                    baseFilters.title(),
                    baseFilters.category(),
                    baseFilters.priceFrom(),
                    baseFilters.priceTo(),
                    baseFilters.city(),
                    baseFilters.dateFrom(),
                    baseFilters.dateTo(),
                    userId,
                    null,
                    baseFilters.address(),
                    baseFilters.limit(),
                    baseFilters.offset()
            );
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(invalidFieldMessage(e.getMessage()));
            return;
        }

        List<EventResponse> events = mongoStore.listEvents(filters);
        ctx.json(new EventsListResponse(events, events.size()));
    }

    private static EventFilters parseEventFilters(Context ctx) {
        String id = readOptionalQueryString(ctx, "id");
        String title = readOptionalQueryString(ctx, "title");
        String category = readOptionalQueryString(ctx, "category");
        String city = readOptionalQueryString(ctx, "city");
        String userId = readOptionalQueryString(ctx, "user_id");
        String username = readOptionalQueryString(ctx, "user");
        String address = readOptionalQueryString(ctx, "address");

        if (category != null && !VALID_EVENT_CATEGORIES.contains(category)) {
            throw new IllegalArgumentException("category");
        }

        Integer priceFrom = parseUnsignedQueryInt(ctx.queryParam("price_from"), "price_from");
        Integer priceTo = parseUnsignedQueryInt(ctx.queryParam("price_to"), "price_to");
        if (priceFrom != null && priceTo != null && priceFrom > priceTo) {
            throw new IllegalArgumentException("price_to");
        }

        LocalDate dateFrom = parseDateQuery(ctx.queryParam("date_from"), "date_from");
        if (dateFrom == null) {
            dateFrom = parseDateQuery(ctx.queryParam("started_date_from"), "started_date_from");
        }

        LocalDate dateTo = parseDateQuery(ctx.queryParam("date_to"), "date_to");
        if (dateTo == null) {
            dateTo = parseDateQuery(ctx.queryParam("started_date_to"), "started_date_to");
        }

        if (dateFrom != null && dateTo != null && dateFrom.isAfter(dateTo)) {
            throw new IllegalArgumentException("date_to");
        }

        Integer limit = parseUnsignedQueryInt(ctx.queryParam("limit"), "limit");
        Integer offsetRaw = parseUnsignedQueryInt(ctx.queryParam("offset"), "offset");
        int offset = offsetRaw == null ? 0 : offsetRaw;

        return new EventFilters(
                id,
                title,
                category,
                priceFrom,
                priceTo,
                city,
                dateFrom,
                dateTo,
                userId,
                username,
                address,
                limit,
                offset
        );
    }

    private static UserFilters parseUserFilters(Context ctx) {
        String id = readOptionalQueryString(ctx, "id");
        String name = readOptionalQueryString(ctx, "name");
        Integer limit = parseUnsignedQueryInt(ctx.queryParam("limit"), "limit");
        Integer offsetRaw = parseUnsignedQueryInt(ctx.queryParam("offset"), "offset");
        int offset = offsetRaw == null ? 0 : offsetRaw;

        return new UserFilters(id, name, limit, offset);
    }

    private static EventPatchRequest parseEventPatchRequest(Context ctx) {
        JsonNode rootNode;
        try {
            rootNode = OBJECT_MAPPER.readTree(ctx.body());
        } catch (Exception e) {
            throw new IllegalArgumentException("category");
        }

        if (rootNode == null || !rootNode.isObject()) {
            throw new IllegalArgumentException("category");
        }

        boolean hasCategory = false;
        String category = null;
        boolean hasPrice = false;
        Integer price = null;
        boolean hasCity = false;
        String city = null;

        if (rootNode.has("category")) {
            JsonNode categoryNode = rootNode.get("category");
            if (categoryNode == null || !categoryNode.isTextual()) {
                throw new IllegalArgumentException("category");
            }

            category = categoryNode.asText().trim();
            if (!VALID_EVENT_CATEGORIES.contains(category)) {
                throw new IllegalArgumentException("category");
            }
            hasCategory = true;
        }

        if (rootNode.has("price")) {
            JsonNode priceNode = rootNode.get("price");
            if (priceNode == null || !priceNode.isIntegralNumber()) {
                throw new IllegalArgumentException("price");
            }

            long parsedPrice = priceNode.longValue();
            if (parsedPrice < 0 || parsedPrice > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("price");
            }

            hasPrice = true;
            price = (int) parsedPrice;
        }

        if (rootNode.has("city")) {
            JsonNode cityNode = rootNode.get("city");
            if (cityNode == null || !cityNode.isTextual()) {
                throw new IllegalArgumentException("city");
            }

            hasCity = true;
            city = cityNode.asText().trim();
        }

        return new EventPatchRequest(
                hasCategory,
                category,
                hasPrice,
                price,
                hasCity,
                city
        );
    }

    private static String readOptionalQueryString(Context ctx, String name) {
        String value = ctx.queryParam(name);
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name);
        }

        return trimmed;
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

    private static boolean isValidRfc3339(String value) {
        try {
            OffsetDateTime.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static LocalDate parseDateQuery(String value, String fieldName) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName);
        }

        try {
            return LocalDate.parse(trimmed, DATE_FILTER_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(fieldName);
        }
    }

    private static Integer parseUnsignedQueryInt(String value, String fieldName) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName);
        }

        long parsed;
        try {
            parsed = Long.parseLong(trimmed);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(fieldName);
        }

        if (parsed < 0 || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(fieldName);
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
            usersCollection.createIndex(Indexes.ascending("full_name"));
            eventsCollection.createIndex(Indexes.ascending("title"));
            eventsCollection.createIndex(
                    Indexes.compoundIndex(
                            Indexes.ascending("title"),
                            Indexes.ascending("created_by")
                    )
            );
            eventsCollection.createIndex(Indexes.ascending("created_by"));
            eventsCollection.createIndex(Indexes.ascending("category"));
            eventsCollection.createIndex(Indexes.ascending("price"));
            eventsCollection.createIndex(Indexes.ascending("location.city"));
            eventsCollection.createIndex(Indexes.ascending("started_at"));
        }

        private UserCreationResult createUser(String fullName, String username, String password) {
            String passwordHash = BCrypt.hashpw(password, BCrypt.gensalt());

            Document document = new Document("full_name", fullName)
                    .append("username", username)
                    .append("password_hash", passwordHash);

            try {
                usersCollection.insertOne(document);
                return UserCreationResult.created(documentIdAsString(document.get("_id")));
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

            return documentIdAsString(user.get("_id"));
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
                return EventCreationResult.created(documentIdAsString(event.get("_id")));
            } catch (MongoWriteException e) {
                if (isDuplicateKey(e)) {
                    return EventCreationResult.conflict();
                }
                throw e;
            }
        }

        private boolean updateEventByOrganizer(String eventId, String organizerId, EventPatchRequest request) {
            Bson filter = Filters.and(
                    buildIdFilter(eventId),
                    Filters.eq("created_by", organizerId)
            );

            Document setUpdates = new Document();
            if (request.hasCategory()) {
                setUpdates.append("category", request.category());
            }
            if (request.hasPrice()) {
                setUpdates.append("price", request.price());
            }
            if (request.hasCity() && !isBlank(request.city())) {
                setUpdates.append("location.city", request.city());
            }

            Document unsetUpdates = new Document();
            if (request.hasCity() && isBlank(request.city())) {
                unsetUpdates.append("location.city", "");
            }

            if (setUpdates.isEmpty() && unsetUpdates.isEmpty()) {
                return eventsCollection.find(filter).first() != null;
            }

            Document update = new Document();
            if (!setUpdates.isEmpty()) {
                update.append("$set", setUpdates);
            }
            if (!unsetUpdates.isEmpty()) {
                update.append("$unset", unsetUpdates);
            }

            return eventsCollection.updateOne(filter, update).getMatchedCount() > 0;
        }

        private EventResponse getEventById(String eventId) {
            if (isBlank(eventId)) {
                return null;
            }

            Document document = eventsCollection.find(buildIdFilter(eventId)).first();
            return document == null ? null : mapEvent(document);
        }

        private List<EventResponse> listEvents(EventFilters filters) {
            Bson filter = buildEventFilter(filters);
            FindIterable<Document> cursor = eventsCollection.find(filter);

            List<EventResponse> filteredEvents = new ArrayList<>();
            for (Document document : cursor) {
                if (!matchesDateFilter(document, filters.dateFrom(), filters.dateTo())) {
                    continue;
                }
                filteredEvents.add(mapEvent(document));
            }

            return applyPagination(filteredEvents, filters.limit(), filters.offset());
        }

        private List<UserResponse> listUsers(UserFilters filters) {
            List<Bson> mongoFilters = new ArrayList<>();
            if (!isBlank(filters.id())) {
                mongoFilters.add(buildIdFilter(filters.id()));
            }
            if (!isBlank(filters.name())) {
                mongoFilters.add(Filters.regex(
                        "full_name",
                        Pattern.compile(Pattern.quote(filters.name()), Pattern.CASE_INSENSITIVE)
                ));
            }

            Bson filter = mongoFilters.isEmpty() ? new Document() : Filters.and(mongoFilters);
            FindIterable<Document> cursor = usersCollection.find(filter);
            if (filters.offset() > 0) {
                cursor = cursor.skip(filters.offset());
            }
            if (filters.limit() != null) {
                cursor = cursor.limit(filters.limit());
            }

            List<UserResponse> users = new ArrayList<>();
            for (Document document : cursor) {
                users.add(mapUser(document));
            }
            return users;
        }

        private UserResponse getUserById(String userId) {
            if (isBlank(userId)) {
                return null;
            }

            Document document = usersCollection.find(buildIdFilter(userId)).first();
            return document == null ? null : mapUser(document);
        }

        private boolean userExists(String userId) {
            if (isBlank(userId)) {
                return false;
            }
            return usersCollection.find(buildIdFilter(userId)).first() != null;
        }

        private Bson buildEventFilter(EventFilters filters) {
            List<Bson> mongoFilters = new ArrayList<>();

            if (!isBlank(filters.id())) {
                mongoFilters.add(buildIdFilter(filters.id()));
            }

            if (!isBlank(filters.title())) {
                mongoFilters.add(Filters.regex(
                        "title",
                        Pattern.compile(Pattern.quote(filters.title()), Pattern.CASE_INSENSITIVE)
                ));
            }

            if (!isBlank(filters.category())) {
                mongoFilters.add(Filters.eq("category", filters.category()));
            }

            if (!isBlank(filters.city())) {
                mongoFilters.add(Filters.eq("location.city", filters.city()));
            }

            if (!isBlank(filters.address())) {
                mongoFilters.add(Filters.regex(
                        "location.address",
                        Pattern.compile(Pattern.quote(filters.address()), Pattern.CASE_INSENSITIVE)
                ));
            }

            if (filters.priceFrom() != null || filters.priceTo() != null) {
                Document priceFilter = new Document();
                if (filters.priceFrom() != null) {
                    priceFilter.append("$gte", filters.priceFrom());
                }
                if (filters.priceTo() != null) {
                    priceFilter.append("$lte", filters.priceTo());
                }
                mongoFilters.add(new Document("price", priceFilter));
            }

            if (!isBlank(filters.userId())) {
                mongoFilters.add(Filters.eq("created_by", filters.userId()));
            }

            if (!isBlank(filters.username())) {
                List<String> userIds = findUserIdsByUsername(filters.username());
                if (userIds.isEmpty()) {
                    mongoFilters.add(Filters.exists("_id", false));
                } else if (userIds.size() == 1) {
                    mongoFilters.add(Filters.eq("created_by", userIds.get(0)));
                } else {
                    mongoFilters.add(Filters.in("created_by", userIds));
                }
            }

            if (mongoFilters.isEmpty()) {
                return new Document();
            }
            return Filters.and(mongoFilters);
        }

        private List<String> findUserIdsByUsername(String username) {
            List<String> userIds = new ArrayList<>();
            for (Document user : usersCollection.find(Filters.eq("username", username))) {
                userIds.add(documentIdAsString(user.get("_id")));
            }
            return userIds;
        }

        private List<EventResponse> applyPagination(List<EventResponse> events, Integer limit, int offset) {
            int start = Math.min(Math.max(offset, 0), events.size());
            int end = events.size();

            if (limit != null) {
                end = Math.min(start + limit, events.size());
            }

            return new ArrayList<>(events.subList(start, end));
        }

        private boolean matchesDateFilter(Document document, LocalDate dateFrom, LocalDate dateTo) {
            if (dateFrom == null && dateTo == null) {
                return true;
            }

            String startedAt = stringValue(document.get("started_at"));
            if (isBlank(startedAt)) {
                return false;
            }

            LocalDate startedDate;
            try {
                startedDate = OffsetDateTime.parse(startedAt).toLocalDate();
            } catch (DateTimeParseException e) {
                return false;
            }

            if (dateFrom != null && startedDate.isBefore(dateFrom)) {
                return false;
            }
            if (dateTo != null && startedDate.isAfter(dateTo)) {
                return false;
            }

            return true;
        }

        private static Bson buildIdFilter(String id) {
            if (ObjectId.isValid(id)) {
                return Filters.or(
                        Filters.eq("_id", new ObjectId(id)),
                        Filters.eq("_id", id)
                );
            }
            return Filters.eq("_id", id);
        }

        private EventResponse mapEvent(Document document) {
            Document locationDocument = document.get("location", Document.class);
            String address = "";
            String city = null;
            if (locationDocument != null) {
                address = defaultString(stringValue(locationDocument.get("address")));
                String rawCity = stringValue(locationDocument.get("city"));
                city = isBlank(rawCity) ? null : rawCity;
            }

            String rawCategory = stringValue(document.get("category"));
            Integer price = intValue(document.get("price"));

            return new EventResponse(
                    documentIdAsString(document.get("_id")),
                    defaultString(stringValue(document.get("title"))),
                    isBlank(rawCategory) ? null : rawCategory,
                    price,
                    defaultString(stringValue(document.get("description"))),
                    new EventLocation(city, address),
                    defaultString(stringValue(document.get("created_at"))),
                    defaultString(stringValue(document.get("created_by"))),
                    defaultString(stringValue(document.get("started_at"))),
                    defaultString(stringValue(document.get("finished_at")))
            );
        }

        private UserResponse mapUser(Document document) {
            return new UserResponse(
                    documentIdAsString(document.get("_id")),
                    defaultString(stringValue(document.get("full_name"))),
                    defaultString(stringValue(document.get("username")))
            );
        }

        private static String documentIdAsString(Object id) {
            if (id instanceof ObjectId objectId) {
                return objectId.toHexString();
            }
            if (id == null) {
                return "";
            }
            return String.valueOf(id);
        }

        private static String stringValue(Object value) {
            return value == null ? null : String.valueOf(value);
        }

        private static Integer intValue(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Integer integer) {
                return integer;
            }
            if (value instanceof Long longValue) {
                if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                    return null;
                }
                return longValue.intValue();
            }
            if (value instanceof Double doubleValue) {
                if (doubleValue % 1 != 0) {
                    return null;
                }
                if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                    return null;
                }
                return doubleValue.intValue();
            }
            if (value instanceof String stringValue && !stringValue.isBlank()) {
                try {
                    return Integer.parseInt(stringValue.trim());
                } catch (NumberFormatException ignored) {
                    return null;
                }
            }
            return null;
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
