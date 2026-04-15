package healthcheck;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public final class GatewayApp {

    private static final Logger log = LoggerFactory.getLogger(GatewayApp.class);
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private static final String APP_PORT_ENV = "APP_PORT";
    private static final String SESSION_TTL_ENV = "APP_USER_SESSION_TTL";
    private static final String SESSION_SERVICE_URL_ENV = "SESSION_SERVICE_URL";
    private static final String USER_SERVICE_URL_ENV = "USER_SERVICE_URL";
    private static final String EVENT_SERVICE_URL_ENV = "EVENT_SERVICE_URL";

    private record HealthResponse(String status) {
    }

    private record MessageResponse(String message) {
    }

    private record UserCreateRequest(String full_name, String username, String password) {
    }

    private record LoginRequest(String username, String password) {
    }

    private record EventCreateRequest(
            String title,
            String address,
            String started_at,
            String finished_at,
            String description
    ) {
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

    private record InternalCreateSessionRequest(String user_id) {
    }

    private record InternalCreateSessionResponse(String sid) {
    }

    private record InternalTouchSessionRequest(String sid) {
    }

    private record InternalTouchSessionResponse(boolean exists) {
    }

    private record InternalBindSessionRequest(String sid, String user_id) {
    }

    private record InternalBindSessionResponse(boolean bound) {
    }

    private record InternalSessionInfoResponse(boolean exists, String user_id) {
    }

    private record InternalCreateUserResponse(String user_id) {
    }

    private record InternalLoginResponse(String user_id) {
    }

    private record InternalCreateEventRequest(
            String title,
            String address,
            String started_at,
            String finished_at,
            String description,
            String created_by
    ) {
    }

    private record InternalEventIdResponse(String id) {
    }

    public static void main(String[] args) {
        int port = ServiceSupport.requirePortEnv(APP_PORT_ENV, log);
        int sessionTtlSeconds = ServiceSupport.requirePositiveIntEnv(SESSION_TTL_ENV, log);
        String sessionServiceUrl = ServiceSupport.requireNonBlankEnv(SESSION_SERVICE_URL_ENV, log);
        String userServiceUrl = ServiceSupport.requireNonBlankEnv(USER_SERVICE_URL_ENV, log);
        String eventServiceUrl = ServiceSupport.requireNonBlankEnv(EVENT_SERVICE_URL_ENV, log);

        Javalin app = Javalin.create();

        app.get("/health", ctx -> {
            String sid = ServiceSupport.readValidSidFromCookie(ctx);
            ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);
            ctx.json(new HealthResponse("ok"));
        });

        app.post("/session", ctx -> handleSession(ctx, sessionServiceUrl, sessionTtlSeconds));
        app.post("/users", ctx -> handleCreateUser(ctx, sessionServiceUrl, userServiceUrl, sessionTtlSeconds));
        app.post("/auth/login", ctx -> handleLogin(ctx, sessionServiceUrl, userServiceUrl, sessionTtlSeconds));
        app.post("/auth/logout", ctx -> handleLogout(ctx, sessionServiceUrl));
        app.post("/events", ctx -> handleCreateEvent(ctx, sessionServiceUrl, eventServiceUrl, sessionTtlSeconds));
        app.get("/events", ctx -> handleListEvents(ctx, eventServiceUrl, sessionTtlSeconds));

        app.start(port);
        log.info("Gateway started on port {}", port);
    }

    private static void handleSession(Context ctx, String sessionServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        if (sid == null) {
            String newSid = createSession(sessionServiceUrl, "");
            if (newSid == null) {
                ctx.status(502).json(new MessageResponse("external dependency failed"));
                return;
            }
            ServiceSupport.setSessionCookie(ctx, newSid, sessionTtlSeconds);
            ctx.status(201);
            return;
        }

        Boolean exists = touchSession(sessionServiceUrl, sid);
        if (exists == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (exists) {
            ServiceSupport.setSessionCookie(ctx, sid, sessionTtlSeconds);
            ctx.status(200);
            return;
        }

        String newSid = createSession(sessionServiceUrl, "");
        if (newSid == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }
        ServiceSupport.setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(201);
    }

    private static void handleCreateUser(
            Context ctx,
            String sessionServiceUrl,
            String userServiceUrl,
            int sessionTtlSeconds
    ) {
        String requestSid = ServiceSupport.readValidSidFromCookie(ctx);
        UserCreateRequest request = ServiceSupport.readBody(ctx, UserCreateRequest.class);

        if (request == null) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"full_name\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.full_name())) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"full_name\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.username())) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.password())) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"password\" field"));
            return;
        }

        DownstreamResponse createUserResponse = postJson(
                userServiceUrl,
                "/internal/users",
                request
        );
        if (createUserResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (createUserResponse.statusCode == 409) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(409).json(new MessageResponse("user already exists"));
            return;
        }

        if (createUserResponse.statusCode == 400) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"full_name\" field"));
            return;
        }

        if (createUserResponse.statusCode != 201) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        InternalCreateUserResponse created = parseBody(createUserResponse.body, InternalCreateUserResponse.class);
        if (created == null || ServiceSupport.isBlank(created.user_id())) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        String newSid = createSession(sessionServiceUrl, created.user_id());
        if (newSid == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ServiceSupport.setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(201);
    }

    private static void handleLogin(
            Context ctx,
            String sessionServiceUrl,
            String userServiceUrl,
            int sessionTtlSeconds
    ) {
        String requestSid = ServiceSupport.readValidSidFromCookie(ctx);
        LoginRequest request = ServiceSupport.readBody(ctx, LoginRequest.class);

        if (request == null) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.username())) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.password())) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"password\" field"));
            return;
        }

        DownstreamResponse loginResponse = postJson(userServiceUrl, "/internal/auth/login", request);
        if (loginResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (loginResponse.statusCode == 401) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(401).json(new MessageResponse("invalid credentials"));
            return;
        }

        if (loginResponse.statusCode == 400) {
            touchSessionIfExists(sessionServiceUrl, requestSid);
            ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);
            ctx.status(400).json(new MessageResponse("invalid \"username\" field"));
            return;
        }

        if (loginResponse.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        InternalLoginResponse login = parseBody(loginResponse.body, InternalLoginResponse.class);
        if (login == null || ServiceSupport.isBlank(login.user_id())) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (requestSid != null) {
            Boolean bound = bindSession(sessionServiceUrl, requestSid, login.user_id());
            if (Boolean.TRUE.equals(bound)) {
                ServiceSupport.setSessionCookie(ctx, requestSid, sessionTtlSeconds);
                ctx.status(204);
                return;
            }
        }

        String newSid = createSession(sessionServiceUrl, login.user_id());
        if (newSid == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ServiceSupport.setSessionCookie(ctx, newSid, sessionTtlSeconds);
        ctx.status(204);
    }

    private static void handleLogout(Context ctx, String sessionServiceUrl) {
        String requestSid = ServiceSupport.readValidSidFromCookie(ctx);
        if (requestSid == null) {
            ctx.status(401);
            return;
        }

        InternalSessionInfoResponse session = getSessionInfo(sessionServiceUrl, requestSid);
        if (session == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (!session.exists() || ServiceSupport.isBlank(session.user_id())) {
            ctx.status(401);
            return;
        }

        if (!deleteSession(sessionServiceUrl, requestSid)) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ServiceSupport.clearSessionCookie(ctx, requestSid);
        ctx.status(204);
    }

    private static void handleCreateEvent(
            Context ctx,
            String sessionServiceUrl,
            String eventServiceUrl,
            int sessionTtlSeconds
    ) {
        String requestSid = ServiceSupport.readValidSidFromCookie(ctx);
        if (requestSid == null) {
            ctx.status(401);
            return;
        }

        ServiceSupport.maybeSetSessionCookie(ctx, requestSid, sessionTtlSeconds);

        InternalSessionInfoResponse session = getSessionInfo(sessionServiceUrl, requestSid);
        if (session == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (!session.exists()) {
            ctx.status(401);
            return;
        }

        touchSessionIfExists(sessionServiceUrl, requestSid);

        if (ServiceSupport.isBlank(session.user_id())) {
            ctx.status(401);
            return;
        }

        EventCreateRequest request = ServiceSupport.readBody(ctx, EventCreateRequest.class);
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

        DownstreamResponse createEventResponse = postJson(
                eventServiceUrl,
                "/internal/events",
                new InternalCreateEventRequest(
                        request.title().trim(),
                        request.address().trim(),
                        request.started_at(),
                        request.finished_at(),
                        request.description() == null ? "" : request.description(),
                        session.user_id()
                )
        );

        if (createEventResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (createEventResponse.statusCode == 409) {
            ctx.status(409).json(new MessageResponse("event already exists"));
            return;
        }

        if (createEventResponse.statusCode == 400) {
            ctx.status(400).json(new MessageResponse("invalid \"title\" field"));
            return;
        }

        if (createEventResponse.statusCode != 201) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        InternalEventIdResponse event = parseBody(createEventResponse.body, InternalEventIdResponse.class);
        if (event == null || ServiceSupport.isBlank(event.id())) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.status(201).json(event);
    }

    private static void handleListEvents(Context ctx, String eventServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

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

        String path = buildEventsPath(ctx.queryParam("title"), limit, offsetRaw);
        DownstreamResponse response = get(eventServiceUrl, path);
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 400) {
            ctx.status(400).json(new MessageResponse("invalid \"limit\" parameter"));
            return;
        }

        if (response.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        EventsListResponse events = parseBody(response.body, EventsListResponse.class);
        if (events == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }
        ctx.json(events);
    }

    private static String buildEventsPath(String title, Integer limit, Integer offset) {
        StringBuilder path = new StringBuilder("/internal/events");
        String separator = "?";

        if (!ServiceSupport.isBlank(title)) {
            path.append(separator)
                    .append("title=")
                    .append(URLEncoder.encode(title, StandardCharsets.UTF_8));
            separator = "&";
        }
        if (limit != null) {
            path.append(separator).append("limit=").append(limit);
            separator = "&";
        }
        if (offset != null) {
            path.append(separator).append("offset=").append(offset);
        }
        return path.toString();
    }

    private static void touchSessionIfExists(String sessionServiceUrl, String sid) {
        if (sid != null) {
            touchSession(sessionServiceUrl, sid);
        }
    }

    private static String createSession(String sessionServiceUrl, String userId) {
        DownstreamResponse response = postJson(
                sessionServiceUrl,
                "/internal/sessions",
                new InternalCreateSessionRequest(ServiceSupport.trimToEmpty(userId))
        );
        if (response == null || response.statusCode != 201) {
            return null;
        }
        InternalCreateSessionResponse created = parseBody(response.body, InternalCreateSessionResponse.class);
        if (created == null || !ServiceSupport.isValidSid(created.sid())) {
            return null;
        }
        return created.sid();
    }

    private static Boolean touchSession(String sessionServiceUrl, String sid) {
        DownstreamResponse response = postJson(
                sessionServiceUrl,
                "/internal/sessions/touch",
                new InternalTouchSessionRequest(sid)
        );
        if (response == null || response.statusCode != 200) {
            return null;
        }
        InternalTouchSessionResponse touched = parseBody(response.body, InternalTouchSessionResponse.class);
        return touched == null ? null : touched.exists();
    }

    private static Boolean bindSession(String sessionServiceUrl, String sid, String userId) {
        DownstreamResponse response = postJson(
                sessionServiceUrl,
                "/internal/sessions/bind",
                new InternalBindSessionRequest(sid, userId)
        );
        if (response == null || response.statusCode != 200) {
            return null;
        }
        InternalBindSessionResponse bind = parseBody(response.body, InternalBindSessionResponse.class);
        return bind == null ? null : bind.bound();
    }

    private static InternalSessionInfoResponse getSessionInfo(String sessionServiceUrl, String sid) {
        DownstreamResponse response = get(sessionServiceUrl, "/internal/sessions/" + sid);
        if (response == null || response.statusCode != 200) {
            return null;
        }
        return parseBody(response.body, InternalSessionInfoResponse.class);
    }

    private static boolean deleteSession(String sessionServiceUrl, String sid) {
        DownstreamResponse response = delete(sessionServiceUrl, "/internal/sessions/" + sid);
        return response != null && response.statusCode == 204;
    }

    private static DownstreamResponse postJson(String baseUrl, String path, Object payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload)))
                    .build();

            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new DownstreamResponse(response.statusCode(), response.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream POST failed: {}{}", baseUrl, path, e);
            return null;
        }
    }

    private static DownstreamResponse get(String baseUrl, String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new DownstreamResponse(response.statusCode(), response.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream GET failed: {}{}", baseUrl, path, e);
            return null;
        }
    }

    private static DownstreamResponse delete(String baseUrl, String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .DELETE()
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new DownstreamResponse(response.statusCode(), response.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream DELETE failed: {}{}", baseUrl, path, e);
            return null;
        }
    }

    private static String normalizeUrl(String baseUrl, String path) {
        String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return normalizedBase + normalizedPath;
    }

    private static <T> T parseBody(String raw, Class<T> clazz) {
        try {
            return JSON.readValue(raw, clazz);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private record DownstreamResponse(int statusCode, String body) {
    }
}