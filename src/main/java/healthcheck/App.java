package healthcheck;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String PORT_ENV = "APP_PORT";
    private static final String SESSION_TTL_ENV = "APP_USER_SESSION_TTL";
    private static final String REDIS_HOST_ENV = "REDIS_HOST";
    private static final String REDIS_PORT_ENV = "REDIS_PORT";
    private static final String REDIS_PASSWORD_ENV = "REDIS_PASSWORD";
    private static final String REDIS_DB_ENV = "REDIS_DB";
    private static final String SESSION_COOKIE_NAME = "X-Session-Id";
    private static final Pattern SID_PATTERN = Pattern.compile("^[0-9a-f]{32}$");
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String CREATE_SESSION_SCRIPT = """
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'created_at', ARGV[1], 'updated_at', ARGV[1])
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

    private record HealthResponse(String status) {
    }

    public static void main(String[] args) {
        int port = requirePort(PORT_ENV);
        int sessionTtlSeconds = requirePositiveInt(SESSION_TTL_ENV);
        String redisHost = requireNonBlank(REDIS_HOST_ENV);
        int redisPort = requirePort(REDIS_PORT_ENV);
        int redisDb = requireNonNegativeInt(REDIS_DB_ENV);
        String redisPassword = System.getenv(REDIS_PASSWORD_ENV);

        SessionStore sessionStore = new SessionStore(redisHost, redisPort, redisPassword, redisDb, sessionTtlSeconds);

        Javalin app = Javalin.create();

        app.get("/health", ctx -> {
            String sid = readValidSidFromCookie(ctx);
            if (sid != null) {
                setSessionCookie(ctx, sid, sessionTtlSeconds);
            }
            ctx.json(new HealthResponse("ok"));
        });

        app.post("/session", ctx -> {
            String sid = readValidSidFromCookie(ctx);
            if (sid == null) {
                String newSid = sessionStore.createSession();
                setSessionCookie(ctx, newSid, sessionTtlSeconds);
                ctx.status(201);
                return;
            }

            if (sessionStore.touchSession(sid)) {
                setSessionCookie(ctx, sid, sessionTtlSeconds);
                ctx.status(200);
                return;
            }

            String newSid = sessionStore.createSession();
            setSessionCookie(ctx, newSid, sessionTtlSeconds);
            ctx.status(201);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            sessionStore.close();
        }));

        app.start(port);
        log.info("Service started on port {}", port);
    }

    private static void setSessionCookie(Context ctx, String sid, int ttlSeconds) {
        ctx.header("Set-Cookie", SESSION_COOKIE_NAME + "=" + sid + "; HttpOnly; Path=/; Max-Age=" + ttlSeconds);
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
                fail("Invalid {}={}.", name, raw);
            }
            return port;
        } catch (NumberFormatException e) {
            fail("Invalid {}={}.", name, raw);
            return -1;
        }
    }

    private static void fail(String message, Object... args) {
        log.error(message, args);
        System.exit(1);
    }

    private static final class SessionStore implements AutoCloseable {

        private final JedisPooled jedis;
        private final int sessionTtlSeconds;

        private SessionStore(String redisHost, int redisPort, String redisPassword, int redisDb, int sessionTtlSeconds) {
            DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                    .database(redisDb);

            if (redisPassword != null && !redisPassword.isBlank()) {
                configBuilder.password(redisPassword);
            }

            this.jedis = new JedisPooled(new HostAndPort(redisHost, redisPort), configBuilder.build());
            this.sessionTtlSeconds = sessionTtlSeconds;
        }

        private String createSession() {
            for (int attempt = 0; attempt < 10; attempt++) {
                String sid = generateSid();
                String now = Instant.now().toString();
                Object result = jedis.eval(CREATE_SESSION_SCRIPT, List.of(redisKey(sid)),
                        List.of(now, String.valueOf(sessionTtlSeconds)));
                if (asLong(result) == 1L) {
                    return sid;
                }
            }

            throw new IllegalStateException("Failed to create session");
        }

        private boolean touchSession(String sid) {
            String now = Instant.now().toString();
            Object result = jedis.eval(TOUCH_SESSION_SCRIPT, List.of(redisKey(sid)),
                    List.of(now, String.valueOf(sessionTtlSeconds)));
            return asLong(result) == 1L;
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