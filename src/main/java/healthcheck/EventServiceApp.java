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
import com.mongodb.client.model.Indexes;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
    private static final String ORGANIZER_HEADER = "X-Organizer-Id";
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
            String organizerId = ServiceSupport.trimToEmpty(ctx.header(ORGANIZER_HEADER));
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

        app.get("/internal/events/{id}", ctx -> {
            String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
            EventResponse event = eventStore.getEventById(eventId);
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

            List<EventResponse> events = eventStore.listEvents(filters);
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

            List<EventResponse> events = eventStore.listEvents(filters);
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

    private static final class EventStore implements AutoCloseable {

        private final MongoClient mongoClient;
        private final MongoCollection<Document> usersCollection;
        private final MongoCollection<Document> eventsCollection;

        private EventStore(String host, int port, String databaseName, String username, String password) {
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

        private EventResponse getEventById(String eventId) {
            if (ServiceSupport.isBlank(eventId)) {
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
            if (dateTo != null && startedDate.isAfter(dateTo)) {
                return false;
            }

            return true;
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
                    ServiceSupport.defaultString(stringValue(document.get("finished_at")))
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
        }
    }
}