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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class EventServiceApp {

    private static final Logger log = LoggerFactory.getLogger(EventServiceApp.class);

    private static final String SERVICE_PORT_ENV = "EVENT_SERVICE_PORT";
    private static final String MONGODB_DATABASE_ENV = "MONGODB_DATABASE";
    private static final String MONGODB_DATABASE_FALLBACK_ENV = "MONGODB_DATABSE";
    private static final String MONGODB_USER_ENV = "MONGODB_USER";
    private static final String MONGODB_PASSWORD_ENV = "MONGODB_PASSWORD";
    private static final String MONGODB_HOST_ENV = "MONGODB_HOST";
    private static final String MONGODB_PORT_ENV = "MONGODB_PORT";

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

    private record EventLocation(String address) {
    }

    private record EventResponse(
            String id,
            String title,
            String description,
            EventLocation location,
            String created_at,
            String created_by,
            String started_at,
            String finished_at
    ) {
    }

    private record EventsListResponse(List<EventResponse> events, int count) {
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
        int port = ServiceSupport.requirePortEnv(SERVICE_PORT_ENV, log);
        String mongoDatabase = requireMongoDatabaseName();
        String mongoUser = ServiceSupport.trimToEmpty(System.getenv(MONGODB_USER_ENV));
        String mongoPassword = ServiceSupport.trimToEmpty(System.getenv(MONGODB_PASSWORD_ENV));
        String mongoHost = ServiceSupport.requireNonBlankEnv(MONGODB_HOST_ENV, log);
        int mongoPort = ServiceSupport.requirePortEnv(MONGODB_PORT_ENV, log);

        EventStore eventStore = new EventStore(mongoHost, mongoPort, mongoDatabase, mongoUser, mongoPassword);
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
                    defaultString(request.description()),
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

        app.get("/internal/events", ctx -> {
            Integer limit;
            try {
                limit = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("limit"));
            } catch (IllegalArgumentException e) {
                ctx.status(400).json(new MessageResponse("invalid \"limit\" parameter"));
                return;
            }

            Integer offsetRaw;
            try {
                offsetRaw = ServiceSupport.parseUnsignedQueryInt(ctx.queryParam("offset"));
            } catch (IllegalArgumentException e) {
                ctx.status(400).json(new MessageResponse("invalid \"offset\" parameter"));
                return;
            }

            int offset = offsetRaw == null ? 0 : offsetRaw;
            String titleFilter = ctx.queryParam("title");
            List<EventResponse> events = eventStore.listEvents(titleFilter, limit, offset);
            ctx.json(new EventsListResponse(events, events.size()));
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            eventStore.close();
        }));

        app.start(port);
        log.info("Event service started on port {}", port);
    }

    private static String requireMongoDatabaseName() {
        String primary = ServiceSupport.trimToEmpty(System.getenv(MONGODB_DATABASE_ENV));
        if (!primary.isBlank()) {
            return primary;
        }

        String fallback = ServiceSupport.trimToEmpty(System.getenv(MONGODB_DATABASE_FALLBACK_ENV));
        if (!fallback.isBlank()) {
            return fallback;
        }

        log.error("Environment variable {} is required.", MONGODB_DATABASE_ENV);
        System.exit(1);
        return "";
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private static final class EventStore implements AutoCloseable {

        private final MongoClient mongoClient;
        private final MongoCollection<Document> eventsCollection;

        private EventStore(String host, int port, String databaseName, String username, String password) {
            this.mongoClient = createMongoClient(host, port, databaseName, username, password);
            MongoDatabase database = mongoClient.getDatabase(databaseName);
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

        private void ensureIndexes() {
            eventsCollection.createIndex(Indexes.ascending("title"), new IndexOptions().unique(true));
            eventsCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("title"), Indexes.ascending("created_by")));
            eventsCollection.createIndex(Indexes.ascending("created_by"));
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
                if (e.getError() != null && e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    return EventCreationResult.conflict();
                }
                throw e;
            }
        }

        private List<EventResponse> listEvents(String titleFilter, Integer limit, int offset) {
            Bson filter = new Document();
            if (!ServiceSupport.isBlank(titleFilter)) {
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

        @Override
        public void close() {
            mongoClient.close();
        }
    }
}