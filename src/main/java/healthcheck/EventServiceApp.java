package healthcheck;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import com.mongodb.client.model.Indexes;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class EventServiceApp {

    private static final Logger log = LoggerFactory.getLogger(EventServiceApp.class);

    private static final class Env {
        private static final String SERVICE_PORT = "EVENT_SERVICE_PORT";
        private static final String MONGODB_DATABASE = "MONGODB_DATABASE";
        private static final String MONGODB_DATABASE_FALLBACK = "MONGODB_DATABSE";
        private static final String MONGODB_USER = "MONGODB_USER";
        private static final String MONGODB_PASSWORD = "MONGODB_PASSWORD";
        private static final String MONGODB_HOST = "MONGODB_HOST";
        private static final String MONGODB_PORT = "MONGODB_PORT";
        private static final String REDIS_HOST = "REDIS_HOST";
        private static final String REDIS_PORT = "REDIS_PORT";
        private static final String REDIS_PASSWORD = "REDIS_PASSWORD";
        private static final String REDIS_DB = "REDIS_DB";
        private static final String LIKE_TTL = "APP_LIKE_TTL";
        private static final String CASSANDRA_HOSTS = "CASSANDRA_HOSTS";
        private static final String CASSANDRA_PORT = "CASSANDRA_PORT";
        private static final String CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
        private static final String CASSANDRA_PASSWORD = "CASSANDRA_PASSWORD";
        private static final String CASSANDRA_KEYSPACE = "CASSANDRA_KEYSPACE";
        private static final String CASSANDRA_CONSISTENCY = "CASSANDRA_CONSISTENCY";
        private static final String CASSANDRA_LOCAL_DC = "CASSANDRA_LOCAL_DATACENTER";

        private Env() {
        }
    }

    private static final class Header {
        private static final String ORGANIZER_ID = "X-Organizer-Id";
        private static final String USER_ID = "X-User-Id";

        private Header() {
        }
    }

    private static final class ReactionConst {
        private static final String TABLE_NAME = "event_reactions";
        private static final String CACHE_KEY_PATTERN = "events:%s:reactions";
        private static final String CACHE_FIELD_LIKES = "likes";
        private static final String CACHE_FIELD_DISLIKES = "dislikes";
        private static final byte LIKE = 1;
        private static final byte DISLIKE = -1;

        private ReactionConst() {
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Set<String> VALID_EVENT_CATEGORIES = Set.of(
            "meetup",
            "concert",
            "exhibition",
            "party",
            "other"
    );

    private record HealthResponse(String status) {
    }

    private record MessageResponse(String message) {
    }

    private record CreateEventRequest(
            String title,
            String address,
            String started_at,
            String finished_at,
            String description,
            String created_by
    ) {
    }

    private record EventIdResponse(String id) {
    }

    private record EventLocation(String city, String address) {
    }

    private record EventReactions(int likes, int dislikes) {
        private static EventReactions empty() {
            return new EventReactions(0, 0);
        }

        private boolean hasAny() {
            return likes > 0 || dislikes > 0;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
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
            String finished_at,
            EventReactions reactions
    ) {
    }

    private record EventsListResponse(List<EventResponse> events, int count) {
    }

    private record EventPatchRequest(
            boolean hasCategory,
            String category,
            boolean hasPrice,
            Integer price,
            boolean hasCity,
            String city
    ) {
    }

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
    ) {
    }

    private record EventCreationResult(boolean created, String eventId) {
        private static EventCreationResult created(String eventId) {
            return new EventCreationResult(true, eventId);
        }

        private static EventCreationResult conflict() {
            return new EventCreationResult(false, null);
        }
    }

    public static void main(String[] args) {
        int port = ServiceSupport.requirePortEnv(Env.SERVICE_PORT, log);
        String mongoDatabase = requireMongoDatabaseName();
        String mongoUser = ServiceSupport.trimToEmpty(System.getenv(Env.MONGODB_USER));
        String mongoPassword = ServiceSupport.trimToEmpty(System.getenv(Env.MONGODB_PASSWORD));
        String mongoHost = ServiceSupport.requireNonBlankEnv(Env.MONGODB_HOST, log);
        int mongoPort = ServiceSupport.requirePortEnv(Env.MONGODB_PORT, log);
        String redisHost = ServiceSupport.requireNonBlankEnv(Env.REDIS_HOST, log);
        int redisPort = ServiceSupport.requirePortEnv(Env.REDIS_PORT, log);
        int redisDb = ServiceSupport.requireNonNegativeIntEnv(Env.REDIS_DB, log);
        String redisPassword = ServiceSupport.trimToEmpty(System.getenv(Env.REDIS_PASSWORD));
        int likeTtlSeconds = ServiceSupport.requirePositiveIntEnv(Env.LIKE_TTL, log);
        String[] cassandraHostsRaw = ServiceSupport.requireNonBlankEnv(Env.CASSANDRA_HOSTS, log).split(",");
        List<String> cassandraHosts = new ArrayList<>();
        for (String cassandraHost : cassandraHostsRaw) {
            String host = ServiceSupport.trimToEmpty(cassandraHost);
            if (!host.isBlank()) {
                cassandraHosts.add(host);
            }
        }
        if (cassandraHosts.isEmpty()) {
            log.error("Environment variable {} must contain at least one host.", Env.CASSANDRA_HOSTS);
            System.exit(1);
        }
        int cassandraPort = ServiceSupport.requirePortEnv(Env.CASSANDRA_PORT, log);
        String cassandraUsername = ServiceSupport.trimToEmpty(System.getenv(Env.CASSANDRA_USERNAME));
        String cassandraPassword = ServiceSupport.trimToEmpty(System.getenv(Env.CASSANDRA_PASSWORD));
        String cassandraKeyspace = ServiceSupport.requireNonBlankEnv(Env.CASSANDRA_KEYSPACE, log).trim();
        String cassandraConsistencyRaw = ServiceSupport.requireNonBlankEnv(Env.CASSANDRA_CONSISTENCY, log).trim();
        String cassandraLocalDatacenter = ServiceSupport.requireNonBlankEnv(Env.CASSANDRA_LOCAL_DC, log).trim();
        ConsistencyLevel cassandraConsistency = parseConsistency(cassandraConsistencyRaw);

        EventStore eventStore = new EventStore(
                mongoHost,
                mongoPort,
                mongoDatabase,
                mongoUser,
                mongoPassword,
                redisHost,
                redisPort,
                redisPassword,
                redisDb,
                likeTtlSeconds,
                cassandraHosts,
                cassandraPort,
                cassandraUsername,
                cassandraPassword,
                cassandraKeyspace,
                cassandraLocalDatacenter,
                cassandraConsistency
        );
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        app.post("/internal/events", ctx -> {
            CreateEventRequest request = ServiceSupport.readBody(ctx, CreateEventRequest.class);
            if (request == null) {
                ctx.status(400).json(new MessageResponse("invalid \"title\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.title())) {
                ctx.status(400).json(new MessageResponse("invalid \"title\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.address())) {
                ctx.status(400).json(new MessageResponse("invalid \"address\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.started_at()) || !ServiceSupport.isValidRfc3339(request.started_at())) {
                ctx.status(400).json(new MessageResponse("invalid \"started_at\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.finished_at()) || !ServiceSupport.isValidRfc3339(request.finished_at())) {
                ctx.status(400).json(new MessageResponse("invalid \"finished_at\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.created_by())) {
                ctx.status(400).json(new MessageResponse("invalid \"created_by\" field"));
                return;
            }

            EventCreationResult result = eventStore.createEvent(
                    request.title().trim(),
                    ServiceSupport.defaultString(request.description()),
                    request.address().trim(),
                    request.started_at(),
                    request.finished_at(),
                    request.created_by().trim()
            );

            if (!result.created()) {
                ctx.status(409).json(new MessageResponse("event already exists"));
                return;
            }

            ctx.status(201).json(new EventIdResponse(result.eventId()));
        });

        app.patch("/internal/events/{id}", ctx -> {
            String organizerId = ServiceSupport.trimToEmpty(ctx.header(Header.ORGANIZER_ID));
            if (ServiceSupport.isBlank(organizerId)) {
                ctx.status(400).json(new MessageResponse("invalid \"created_by\" field"));
                return;
            }

            EventPatchRequest patchRequest;
            try {
                patchRequest = parseEventPatchRequest(ctx);
            } catch (IllegalArgumentException e) {
                ctx.status(400).json(new MessageResponse("invalid \"" + e.getMessage() + "\" field"));
                return;
            }

            String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            if (ServiceSupport.isBlank(eventId)) {
                ctx.status(404).json(new MessageResponse("Not found. Be sure that event exists and you are the organizer"));
                return;
            }

            boolean updated = eventStore.updateEventByOrganizer(eventId, organizerId, patchRequest);
            if (!updated) {
                ctx.status(404).json(new MessageResponse("Not found. Be sure that event exists and you are the organizer"));
                return;
            }

            ctx.status(204);
        });

        app.post("/internal/events/{id}/like", ctx -> {
            String userId = ServiceSupport.trimToEmpty(ctx.header(Header.USER_ID));
            if (ServiceSupport.isBlank(userId)) {
                ctx.status(400).json(new MessageResponse("invalid \"user_id\" field"));
                return;
            }

            String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            if (!eventStore.setReaction(eventId, userId, ReactionConst.LIKE)) {
                ctx.status(404).json(new MessageResponse("Event not found"));
                return;
            }

            ctx.status(204);
        });

        app.post("/internal/events/{id}/dislike", ctx -> {
            String userId = ServiceSupport.trimToEmpty(ctx.header(Header.USER_ID));
            if (ServiceSupport.isBlank(userId)) {
                ctx.status(400).json(new MessageResponse("invalid \"user_id\" field"));
                return;
            }

            String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            if (!eventStore.setReaction(eventId, userId, ReactionConst.DISLIKE)) {
                ctx.status(404).json(new MessageResponse("Event not found"));
                return;
            }

            ctx.status(204);
        });

        app.get("/internal/events/{id}", ctx -> {
            String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            boolean includeReactions = isIncludeReactionsRequested(ctx);
            EventResponse event = eventStore.getEventById(eventId, includeReactions);
            if (event == null) {
                ctx.status(404).json(new MessageResponse("Not found"));
                return;
            }
            ctx.json(event);
        });

        app.get("/internal/events", ctx -> {
            EventFilters filters;
            try {
                filters = parseEventFilters(ctx);
            } catch (IllegalArgumentException e) {
                ctx.status(400).json(new MessageResponse("invalid \"" + e.getMessage() + "\" field"));
                return;
            }

            boolean includeReactions = isIncludeReactionsRequested(ctx);
            List<EventResponse> events = eventStore.listEvents(filters, includeReactions);
            ctx.json(new EventsListResponse(events, events.size()));
        });

        app.get("/internal/users/{id}/events", ctx -> {
            String userId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            if (!eventStore.userExists(userId)) {
                ctx.status(404).json(new MessageResponse("User not found"));
                return;
            }

            EventFilters filters;
            try {
                EventFilters base = parseEventFilters(ctx);
                filters = new EventFilters(
                        base.id(),
                        base.title(),
                        base.category(),
                        base.priceFrom(),
                        base.priceTo(),
                        base.city(),
                        base.dateFrom(),
                        base.dateTo(),
                        userId,
                        null,
                        base.address(),
                        base.limit(),
                        base.offset()
                );
            } catch (IllegalArgumentException e) {
                ctx.status(400).json(new MessageResponse("invalid \"" + e.getMessage() + "\" field"));
                return;
            }

            boolean includeReactions = isIncludeReactionsRequested(ctx);
            List<EventResponse> events = eventStore.listEvents(filters, includeReactions);
            ctx.json(new EventsListResponse(events, events.size()));
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            eventStore.close();
        }));

        app.start(port);
        log.info("Event service started on port {}", port);
    }

    private static EventFilters parseEventFilters(Context ctx) {
        String id = ServiceSupport.readOptionalQueryString(ctx, "id");
        String title = ServiceSupport.readOptionalQueryString(ctx, "title");
        String category = ServiceSupport.readOptionalQueryString(ctx, "category");
        String city = ServiceSupport.readOptionalQueryString(ctx, "city");
        String userId = ServiceSupport.readOptionalQueryString(ctx, "user_id");
        String username = ServiceSupport.readOptionalQueryString(ctx, "user");
        String address = ServiceSupport.readOptionalQueryString(ctx, "address");

        if (category != null && !VALID_EVENT_CATEGORIES.contains(category)) {
            throw new IllegalArgumentException("category");
        }

        Integer priceFrom = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("price_from"), "price_from");
        Integer priceTo = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("price_to"), "price_to");
        if (priceFrom != null && priceTo != null && priceFrom > priceTo) {
            throw new IllegalArgumentException("price_to");
        }

        LocalDate dateFrom = ServiceSupport.parseDateQuery(ctx.queryParam("date_from"), "date_from");
        if (dateFrom == null) {
            dateFrom = ServiceSupport.parseDateQuery(ctx.queryParam("started_date_from"), "started_date_from");
        }

        LocalDate dateTo = ServiceSupport.parseDateQuery(ctx.queryParam("date_to"), "date_to");
        if (dateTo == null) {
            dateTo = ServiceSupport.parseDateQuery(ctx.queryParam("started_date_to"), "started_date_to");
        }

        if (dateFrom != null && dateTo != null && dateFrom.isAfter(dateTo)) {
            throw new IllegalArgumentException("date_to");
        }

        Integer limit = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("limit"), "limit");
        Integer offsetRaw = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("offset"), "offset");
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

    private static String requireMongoDatabaseName() {
        String primary = ServiceSupport.trimToEmpty(System.getenv(Env.MONGODB_DATABASE));
        if (!primary.isBlank()) {
            return primary;
        }

        String fallback = ServiceSupport.trimToEmpty(System.getenv(Env.MONGODB_DATABASE_FALLBACK));
        if (!fallback.isBlank()) {
            return fallback;
        }

        log.error("Environment variable {} is required.", Env.MONGODB_DATABASE);
        System.exit(1);
        return "";
    }

    private static ConsistencyLevel parseConsistency(String raw) {
        try {
            return DefaultConsistencyLevel.valueOf(raw.trim().toUpperCase());
        } catch (Exception e) {
            log.error("Invalid {}={}", Env.CASSANDRA_CONSISTENCY, raw);
            System.exit(1);
            return DefaultConsistencyLevel.ONE;
        }
    }

    private static boolean isIncludeReactionsRequested(Context ctx) {
        List<String> includes = ctx.queryParams("include");
        for (String include : includes) {
            if (include == null) {
                continue;
            }
            String[] tokens = include.split(",");
            for (String token : tokens) {
                if ("reactions".equals(token.trim())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final class EventStore implements AutoCloseable {

        private final MongoClient mongoClient;
        private final MongoCollection<Document> usersCollection;
        private final MongoCollection<Document> eventsCollection;
        private final CqlSession cassandraSession;
        private final PreparedStatement upsertReactionStatement;
        private final PreparedStatement selectReactionByEventStatement;
        private final ConsistencyLevel cassandraConsistency;
        private final JedisPooled reactionsCache;
        private final int likeTtlSeconds;

        private EventStore(
                String mongoHost,
                int mongoPort,
                String mongoDatabaseName,
                String mongoUsername,
                String mongoPassword,
                String redisHost,
                int redisPort,
                String redisPassword,
                int redisDb,
                int likeTtlSeconds,
                List<String> cassandraHosts,
                int cassandraPort,
                String cassandraUsername,
                String cassandraPassword,
                String cassandraKeyspace,
                String cassandraLocalDatacenter,
                ConsistencyLevel cassandraConsistency
        ) {
            this.mongoClient = createMongoClient(mongoHost, mongoPort, mongoDatabaseName, mongoUsername, mongoPassword);
            MongoDatabase database = mongoClient.getDatabase(mongoDatabaseName);
            this.usersCollection = database.getCollection("users");
            this.eventsCollection = database.getCollection("events");
            ensureIndexes();

            this.cassandraConsistency = cassandraConsistency;
            this.cassandraSession = createCassandraSession(
                    cassandraHosts,
                    cassandraPort,
                    cassandraUsername,
                    cassandraPassword,
                    cassandraLocalDatacenter
            );
            String reactionsTable = ensureCassandraSchema(cassandraKeyspace);
            this.upsertReactionStatement = cassandraSession.prepare(
                    "INSERT INTO " + reactionsTable + " (event_id, created_by, like_value, created_at) VALUES (?, ?, ?, ?)"
            );
            this.selectReactionByEventStatement = cassandraSession.prepare(
                    "SELECT like_value FROM " + reactionsTable + " WHERE event_id = ?"
            );

            DefaultJedisClientConfig.Builder redisConfig = DefaultJedisClientConfig.builder().database(redisDb);
            if (!redisPassword.isBlank()) {
                redisConfig.password(redisPassword);
            }
            this.reactionsCache = new JedisPooled(new HostAndPort(redisHost, redisPort), redisConfig.build());
            this.likeTtlSeconds = likeTtlSeconds;
        }

        private static MongoClient createMongoClient(String host, int port, String databaseName, String username, String password) {
            if (username.isBlank()) {
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyToClusterSettings(builder -> builder.hosts(List.of(new ServerAddress(host, port))))
                        .build();
                return MongoClients.create(settings);
            }

            MongoClient withDatabaseAuth = MongoClients.create(buildSettings(host, port,
                    MongoCredential.createCredential(username, databaseName, password.toCharArray())));
            if (ping(withDatabaseAuth, databaseName)) {
                return withDatabaseAuth;
            }
            withDatabaseAuth.close();

            MongoClient withAdminAuth = MongoClients.create(buildSettings(host, port,
                    MongoCredential.createCredential(username, "admin", password.toCharArray())));
            if (ping(withAdminAuth, databaseName)) {
                return withAdminAuth;
            }
            withAdminAuth.close();
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

        private static CqlSession createCassandraSession(
                List<String> cassandraHosts,
                int cassandraPort,
                String cassandraUsername,
                String cassandraPassword,
                String cassandraLocalDatacenter
        ) {
            var builder = CqlSession.builder()
                    .withLocalDatacenter(cassandraLocalDatacenter);

            for (String host : cassandraHosts) {
                builder.addContactPoint(new InetSocketAddress(host, cassandraPort));
            }

            if (!cassandraUsername.isBlank()) {
                builder.withAuthCredentials(cassandraUsername, cassandraPassword);
            }

            return builder.build();
        }

        private String ensureCassandraSchema(String keyspaceName) {
            String table = keyspaceName + "." + ReactionConst.TABLE_NAME;

            cassandraSession.execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
                            + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
            );
            cassandraSession.execute(
                    "CREATE TABLE IF NOT EXISTS " + table + " ("
                            + "event_id text, "
                            + "created_by text, "
                            + "like_value tinyint, "
                            + "created_at timestamp, "
                            + "PRIMARY KEY ((event_id), created_by)"
                            + ")"
            );

            return table;
        }

        private void ensureIndexes() {
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
            if (request.hasCity() && !ServiceSupport.isBlank(request.city())) {
                setUpdates.append("location.city", request.city());
            }

            Document unsetUpdates = new Document();
            if (request.hasCity() && ServiceSupport.isBlank(request.city())) {
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

        private boolean setReaction(String eventId, String userId, byte reactionValue) {
            if (ServiceSupport.isBlank(eventId) || ServiceSupport.isBlank(userId)) {
                return false;
            }

            Document event = eventsCollection.find(buildIdFilter(eventId)).first();
            if (event == null) {
                return false;
            }

            String normalizedEventId = documentIdAsString(event.get("_id"));
            BoundStatement statement = upsertReactionStatement
                    .bind(normalizedEventId, userId, reactionValue, Instant.now())
                    .setConsistencyLevel(cassandraConsistency);
            cassandraSession.execute(statement);

            String title = stringValue(event.get("title"));
            refreshReactionsCacheByTitle(title);
            return true;
        }

        private EventResponse getEventById(String eventId, boolean includeReactions) {
            if (ServiceSupport.isBlank(eventId)) {
                return null;
            }

            Document document = eventsCollection.find(buildIdFilter(eventId)).first();
            if (document == null) {
                return null;
            }

            EventResponse event = mapEvent(document);
            if (!includeReactions) {
                return event;
            }

            return withReactions(event, readReactionsByTitle(event.title()));
        }

        private List<EventResponse> listEvents(EventFilters filters, boolean includeReactions) {
            Bson filter = buildEventFilter(filters);
            FindIterable<Document> cursor = eventsCollection.find(filter);

            List<EventResponse> filteredEvents = new ArrayList<>();
            for (Document document : cursor) {
                if (!matchesDateFilter(document, filters.dateFrom(), filters.dateTo())) {
                    continue;
                }
                filteredEvents.add(mapEvent(document));
            }

            List<EventResponse> paginated = applyPagination(filteredEvents, filters.limit(), filters.offset());
            if (!includeReactions) {
                return paginated;
            }
            return attachReactions(paginated);
        }

        private List<EventResponse> attachReactions(List<EventResponse> events) {
            if (events.isEmpty()) {
                return events;
            }

            Map<String, EventReactions> reactionsByTitle = new HashMap<>();
            List<EventResponse> withReactions = new ArrayList<>(events.size());
            for (EventResponse event : events) {
                EventReactions reactions = reactionsByTitle.computeIfAbsent(
                        event.title(),
                        this::readReactionsByTitle
                );
                withReactions.add(withReactions(event, reactions));
            }
            return withReactions;
        }

        private static EventResponse withReactions(EventResponse event, EventReactions reactions) {
            return new EventResponse(
                    event.id(),
                    event.title(),
                    event.category(),
                    event.price(),
                    event.description(),
                    event.location(),
                    event.created_at(),
                    event.created_by(),
                    event.started_at(),
                    event.finished_at(),
                    reactions
            );
        }

        private EventReactions readReactionsByTitle(String title) {
            if (ServiceSupport.isBlank(title)) {
                return EventReactions.empty();
            }

            EventReactions cached = readReactionsFromCache(title);
            if (cached != null) {
                return cached;
            }

            List<String> eventIds = findAllEventIdsByTitle(title);
            if (eventIds.isEmpty()) {
                return EventReactions.empty();
            }

            EventReactions calculated = readReactionsFromCassandra(eventIds);
            if (calculated.hasAny()) {
                storeReactionsInCache(title, calculated);
            }
            return calculated;
        }

        private EventReactions readReactionsFromCache(String title) {
            return readReactionsFromCacheKey(titleReactionsCacheKey(title));
        }

        private EventReactions readReactionsFromCacheKey(String cacheKey) {
            Map<String, String> values = reactionsCache.hgetAll(cacheKey);
            if (values == null || values.isEmpty()) {
                return null;
            }

            Integer likes = parseNonNegativeInt(values.get(ReactionConst.CACHE_FIELD_LIKES));
            Integer dislikes = parseNonNegativeInt(values.get(ReactionConst.CACHE_FIELD_DISLIKES));
            if (likes == null || dislikes == null) {
                reactionsCache.del(cacheKey);
                return null;
            }

            return new EventReactions(likes, dislikes);
        }

        private void storeReactionsInCache(String title, EventReactions reactions) {
            storeReactionsInCacheByKey(titleReactionsCacheKey(title), reactions);
        }

        private void storeReactionsInCacheByKey(String cacheKey, EventReactions reactions) {
            try {
                Map<String, String> values = Map.of(
                        ReactionConst.CACHE_FIELD_LIKES, String.valueOf(reactions.likes()),
                        ReactionConst.CACHE_FIELD_DISLIKES, String.valueOf(reactions.dislikes())
                );
                reactionsCache.hset(cacheKey, values);
                reactionsCache.expire(cacheKey, likeTtlSeconds);
            } catch (Exception e) {
                log.warn("Failed to write reactions cache for key {}", cacheKey, e);
            }
        }

        private void refreshReactionsCacheByTitle(String title) {
            if (ServiceSupport.isBlank(title)) {
                return;
            }

            List<String> eventIds = findAllEventIdsByTitle(title);
            if (eventIds.isEmpty()) {
                invalidateTitleReactionsCache(title);
                return;
            }

            EventReactions reactions = readReactionsFromCassandra(eventIds);
            if (!reactions.hasAny()) {
                invalidateTitleReactionsCache(title);
                return;
            }

            storeReactionsInCache(title, reactions);
        }

        private void invalidateTitleReactionsCache(String title) {
            if (ServiceSupport.isBlank(title)) {
                return;
            }
            reactionsCache.del(titleReactionsCacheKey(title));
        }

        private static String titleReactionsCacheKey(String title) {
            return ReactionConst.CACHE_KEY_PATTERN.formatted(md5Hex(title));
        }

        private static String md5Hex(String value) {
            try {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] digest = md5.digest(value.getBytes(StandardCharsets.UTF_8));
                return HexFormat.of().formatHex(digest);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("MD5 is not available", e);
            }
        }

        private List<String> findAllEventIdsByTitle(String title) {
            List<String> eventIds = new ArrayList<>();
            for (Document event : eventsCollection.find(Filters.eq("title", title)).projection(new Document("_id", 1))) {
                eventIds.add(documentIdAsString(event.get("_id")));
            }
            return eventIds;
        }

        private EventReactions readReactionsFromCassandra(List<String> eventIds) {
            int likes = 0;
            int dislikes = 0;

            for (String eventId : eventIds) {
                BoundStatement statement = selectReactionByEventStatement
                        .bind(eventId)
                        .setConsistencyLevel(cassandraConsistency);
                ResultSet result = cassandraSession.execute(statement);

                for (Row row : result) {
                    Byte value = row.getByte("like_value");
                    if (value == null) {
                        continue;
                    }
                    if (value == ReactionConst.LIKE) {
                        likes++;
                        continue;
                    }
                    if (value == ReactionConst.DISLIKE) {
                        dislikes++;
                    }
                }
            }

            return new EventReactions(likes, dislikes);
        }

        private boolean userExists(String userId) {
            if (ServiceSupport.isBlank(userId)) {
                return false;
            }
            return usersCollection.find(buildIdFilter(userId)).first() != null;
        }

        private Bson buildEventFilter(EventFilters filters) {
            List<Bson> mongoFilters = new ArrayList<>();

            if (!ServiceSupport.isBlank(filters.id())) {
                mongoFilters.add(buildIdFilter(filters.id()));
            }

            if (!ServiceSupport.isBlank(filters.title())) {
                mongoFilters.add(Filters.regex(
                        "title",
                        Pattern.compile(Pattern.quote(filters.title()), Pattern.CASE_INSENSITIVE)
                ));
            }

            if (!ServiceSupport.isBlank(filters.category())) {
                mongoFilters.add(Filters.eq("category", filters.category()));
            }

            if (!ServiceSupport.isBlank(filters.city())) {
                mongoFilters.add(Filters.eq("location.city", filters.city()));
            }

            if (!ServiceSupport.isBlank(filters.address())) {
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

            if (!ServiceSupport.isBlank(filters.userId())) {
                mongoFilters.add(Filters.eq("created_by", filters.userId()));
            }

            if (!ServiceSupport.isBlank(filters.username())) {
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
            if (ServiceSupport.isBlank(startedAt)) {
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
            return dateTo == null || !startedDate.isAfter(dateTo);
        }

        private EventResponse mapEvent(Document document) {
            Document locationDocument = document.get("location", Document.class);
            String address = "";
            String city = null;
            if (locationDocument != null) {
                address = ServiceSupport.defaultString(stringValue(locationDocument.get("address")));
                String rawCity = stringValue(locationDocument.get("city"));
                city = ServiceSupport.isBlank(rawCity) ? null : rawCity;
            }

            String rawCategory = stringValue(document.get("category"));
            Integer price = intValue(document.get("price"));

            return new EventResponse(
                    documentIdAsString(document.get("_id")),
                    ServiceSupport.defaultString(stringValue(document.get("title"))),
                    ServiceSupport.isBlank(rawCategory) ? null : rawCategory,
                    price,
                    ServiceSupport.defaultString(stringValue(document.get("description"))),
                    new EventLocation(city, address),
                    ServiceSupport.defaultString(stringValue(document.get("created_at"))),
                    ServiceSupport.defaultString(stringValue(document.get("created_by"))),
                    ServiceSupport.defaultString(stringValue(document.get("started_at"))),
                    ServiceSupport.defaultString(stringValue(document.get("finished_at"))),
                    null
            );
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

        private static Integer parseNonNegativeInt(String value) {
            if (ServiceSupport.isBlank(value)) {
                return null;
            }
            try {
                int parsed = Integer.parseInt(value.trim());
                return parsed < 0 ? null : parsed;
            } catch (NumberFormatException e) {
                return null;
            }
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
            return e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY;
        }

        @Override
        public void close() {
            mongoClient.close();
            cassandraSession.close();
            reactionsCache.close();
        }
    }
}