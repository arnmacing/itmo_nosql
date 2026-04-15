package healthcheck;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

public final class SessionServiceApp {

    private static final Logger log = LoggerFactory.getLogger(SessionServiceApp.class);

    private static final String SERVICE_PORT_ENV = "SESSION_SERVICE_PORT";
    private static final String SESSION_TTL_ENV = "APP_USER_SESSION_TTL";
    private static final String REDIS_HOST_ENV = "REDIS_HOST";
    private static final String REDIS_PORT_ENV = "REDIS_PORT";
    private static final String REDIS_PASSWORD_ENV = "REDIS_PASSWORD";
    private static final String REDIS_DB_ENV = "REDIS_DB";

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

    private record MessageResponse(String message) {
    }

    private record CreateSessionRequest(String user_id) {
    }

    private record CreateSessionResponse(String sid) {
    }

    private record SidRequest(String sid) {
    }

    private record TouchSessionResponse(boolean exists) {
    }

    private record BindSessionRequest(String sid, String user_id) {
    }

    private record BindSessionResponse(boolean bound) {
    }

    private record SessionInfoResponse(boolean exists, String user_id) {
    }

    private record SessionInfo(boolean exists, String userId) {
        private static SessionInfo missing() {
            return new SessionInfo(false, null);
        }

        private static SessionInfo present(String userId) {
            return new SessionInfo(true, userId);
        }
    }

    public static void main(String[] args) {
        int port = ServiceSupport.requirePortEnv(SERVICE_PORT_ENV, log);
        int sessionTtlSeconds = ServiceSupport.requirePositiveIntEnv(SESSION_TTL_ENV, log);
        String redisHost = ServiceSupport.requireNonBlankEnv(REDIS_HOST_ENV, log);
        int redisPort = ServiceSupport.requirePortEnv(REDIS_PORT_ENV, log);
        int redisDb = ServiceSupport.requireNonNegativeIntEnv(REDIS_DB_ENV, log);
        String redisPassword = ServiceSupport.trimToEmpty(System.getenv(REDIS_PASSWORD_ENV));

        SessionStore sessionStore = new SessionStore(redisHost, redisPort, redisPassword, redisDb, sessionTtlSeconds);
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        app.post("/internal/sessions", ctx -> {
            CreateSessionRequest request = ServiceSupport.readBody(ctx, CreateSessionRequest.class);
            String userId = request == null ? "" : ServiceSupport.trimToEmpty(request.user_id());
            String sid = sessionStore.createSessionWithOptionalUser(userId);
            ctx.status(201).json(new CreateSessionResponse(sid));
        });

        app.post("/internal/sessions/touch", ctx -> {
            SidRequest request = ServiceSupport.readBody(ctx, SidRequest.class);
            if (request == null || !ServiceSupport.isValidSid(request.sid())) {
                ctx.status(400).json(new MessageResponse("invalid \"sid\" field"));
                return;
            }
            boolean exists = sessionStore.touchSession(request.sid());
            ctx.json(new TouchSessionResponse(exists));
        });

        app.post("/internal/sessions/bind", ctx -> {
            BindSessionRequest request = ServiceSupport.readBody(ctx, BindSessionRequest.class);
            if (request == null || !ServiceSupport.isValidSid(request.sid())) {
                ctx.status(400).json(new MessageResponse("invalid \"sid\" field"));
                return;
            }
            if (ServiceSupport.isBlank(request.user_id())) {
                ctx.status(400).json(new MessageResponse("invalid \"user_id\" field"));
                return;
            }
            boolean bound = sessionStore.bindSessionToUser(request.sid(), request.user_id().trim());
            ctx.json(new BindSessionResponse(bound));
        });

        app.get("/internal/sessions/{sid}", ctx -> {
            String sid = ctx.pathParam("sid");
            if (!ServiceSupport.isValidSid(sid)) {
                ctx.json(new SessionInfoResponse(false, null));
                return;
            }

            SessionInfo session = sessionStore.readSession(sid);
            ctx.json(new SessionInfoResponse(session.exists(), session.userId()));
        });

        app.delete("/internal/sessions/{sid}", ctx -> {
            String sid = ctx.pathParam("sid");
            if (ServiceSupport.isValidSid(sid)) {
                sessionStore.deleteSession(sid);
            }
            ctx.status(204);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            sessionStore.close();
        }));

        app.start(port);
        log.info("Session service started on port {}", port);
    }

    private static String generateSid() {
        byte[] bytes = new byte[16];
        SECURE_RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
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
}