package healthcheck;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.javalin.Javalin;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class UserServiceApp {

    private static final Logger log = LoggerFactory.getLogger(UserServiceApp.class);

    private static final String SERVICE_PORT_ENV = "USER_SERVICE_PORT";
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

    private record CreateUserRequest(String full_name, String username, String password) {
    }

    private record CreateUserResponse(String user_id) {
    }

    private record LoginRequest(String username, String password) {
    }

    private record LoginResponse(String user_id) {
    }

    private record UserCreationResult(boolean created, String userId) {
        private static UserCreationResult created(String userId) {
            return new UserCreationResult(true, userId);
        }

        private static UserCreationResult conflict() {
            return new UserCreationResult(false, null);
        }
    }

    public static void main(String[] args) {
        int port = ServiceSupport.requirePortEnv(SERVICE_PORT_ENV, log);
        String mongoDatabase = requireMongoDatabaseName();
        String mongoUser = ServiceSupport.trimToEmpty(System.getenv(MONGODB_USER_ENV));
        String mongoPassword = ServiceSupport.trimToEmpty(System.getenv(MONGODB_PASSWORD_ENV));
        String mongoHost = ServiceSupport.requireNonBlankEnv(MONGODB_HOST_ENV, log);
        int mongoPort = ServiceSupport.requirePortEnv(MONGODB_PORT_ENV, log);

        UserStore userStore = new UserStore(mongoHost, mongoPort, mongoDatabase, mongoUser, mongoPassword);
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        app.post("/internal/users", ctx -> {
            CreateUserRequest request = ServiceSupport.readBody(ctx, CreateUserRequest.class);
            if (request == null) {
                ctx.status(400).json(new MessageResponse("invalid \"full_name\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.full_name())) {
                ctx.status(400).json(new MessageResponse("invalid \"full_name\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.username())) {
                ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
                return;
            }

            if (ServiceSupport.isBlank(request.password())) {
                ctx.status(400).json(new MessageResponse("invalid \"password\" field"));
                return;
            }

            UserCreationResult created = userStore.createUser(
                    request.full_name().trim(),
                    request.username().trim(),
                    request.password()
            );

            if (!created.created()) {
                ctx.status(409).json(new MessageResponse("user already exists"));
                return;
            }

            ctx.status(201).json(new CreateUserResponse(created.userId()));
        });

        app.post("/internal/auth/login", ctx -> {
            LoginRequest request = ServiceSupport.readBody(ctx, LoginRequest.class);
            if (request == null || ServiceSupport.isBlank(request.username()) || ServiceSupport.isBlank(request.password())) {
                ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
                return;
            }

            String userId = userStore.findUserIdByCredentials(request.username().trim(), request.password());
            if (userId == null) {
                ctx.status(401).json(new MessageResponse("invalid credentials"));
                return;
            }

            ctx.json(new LoginResponse(userId));
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            userStore.close();
        }));

        app.start(port);
        log.info("User service started on port {}", port);
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

    private static final class UserStore implements AutoCloseable {

        private final MongoClient mongoClient;
        private final MongoCollection<Document> usersCollection;

        private UserStore(String host, int port, String databaseName, String username, String password) {
            this.mongoClient = createMongoClient(host, port, databaseName, username, password);
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            this.usersCollection = database.getCollection("users");
            this.usersCollection.createIndex(Indexes.ascending("username"), new IndexOptions().unique(true));
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
                if (e.getError() != null && e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
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
            if (ServiceSupport.isBlank(hash)) {
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

        @Override
        public void close() {
            mongoClient.close();
        }
    }
}