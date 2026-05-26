package healthcheck;

import com.fasterxml.jackson.annotation.JsonInclude;
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
    private static final String ORGANIZER_HEADER = "X-Organizer-Id";
    private static final String REACTION_USER_HEADER = "X-User-Id";

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

    private record EventLocation(String city, String address) {
    }

    private record EventReactions(int likes, int dislikes) {
    }

    private record EventReviews(int count, double rating) {
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
            EventReactions reactions,
            EventReviews reviews
    ) {
    }

    private record CreateReviewRequest(String comment, Integer rating) {
    }

    private record UpdateReviewRequest(String comment, Integer rating) {
    }

    private record ReviewIdResponse(String id) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record ReviewResponse(
            String id,
            String event_id,
            String comment,
            String created_at,
            String created_by,
            int rating,
            String updated_at
    ) {
    }

    private record ReviewsListResponse(List<ReviewResponse> reviews, int count) {
    }

    private record EventsListResponse(List<EventResponse> events, int count) {
    }

    private record UserResponse(String id, String full_name, String username) {
    }

    private record UsersListResponse(List<UserResponse> users, int count) {
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
        app.patch("/events/{id}", ctx -> handlePatchEvent(ctx, sessionServiceUrl, eventServiceUrl, sessionTtlSeconds));
        app.post("/events/{id}/like", ctx -> handleEventReaction(
                ctx,
                sessionServiceUrl,
                eventServiceUrl,
                sessionTtlSeconds,
                "like",
                false
        ));
        app.post("/events/{id}/dislike", ctx -> handleEventReaction(
                ctx,
                sessionServiceUrl,
                eventServiceUrl,
                sessionTtlSeconds,
                "dislike",
                true
        ));
        app.get("/events/{id}", ctx -> handleGetEvent(ctx, eventServiceUrl, sessionTtlSeconds));
        app.get("/events", ctx -> handleListEvents(ctx, eventServiceUrl, sessionTtlSeconds));
        app.get("/users/{id}/events", ctx -> handleListUserEvents(ctx, eventServiceUrl, sessionTtlSeconds));
        app.get("/users/{id}", ctx -> handleGetUser(ctx, userServiceUrl, sessionTtlSeconds));
        app.get("/users", ctx -> handleListUsers(ctx, userServiceUrl, sessionTtlSeconds));
        app.post("/events/{event_id}/reviews", ctx -> handleCreateReview(ctx, sessionServiceUrl, eventServiceUrl, sessionTtlSeconds));
        app.get("/events/{event_id}/reviews", ctx -> handleListReviews(ctx, eventServiceUrl, sessionTtlSeconds));
        app.patch("/events/{event_id}/reviews/{review_id}", ctx -> handleUpdateReview(ctx, sessionServiceUrl, eventServiceUrl, sessionTtlSeconds));

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

        DownstreamResponse createUserResponse = postJson(userServiceUrl, "/internal/users", request);
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
            ctx.status(400).json(readMessageOrFallback(createUserResponse.body, "invalid \"full_name\" field"));
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
            ctx.status(400).json(readMessageOrFallback(loginResponse.body, "invalid \"username\" field"));
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
            ctx.status(400).json(readMessageOrFallback(createEventResponse.body, "invalid \"title\" field"));
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

    private static void handlePatchEvent(
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

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
        if (ServiceSupport.isBlank(eventId)) {
            ctx.status(404).json(new MessageResponse("Not found. Be sure that event exists and you are the organizer"));
            return;
        }

        DownstreamResponse patchResponse = patchRaw(
                eventServiceUrl,
                "/internal/events/" + encodePathSegment(eventId),
                ctx.body(),
                ORGANIZER_HEADER,
                session.user_id()
        );

        if (patchResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (patchResponse.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(patchResponse.body, "invalid \"category\" field"));
            return;
        }

        if (patchResponse.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(
                    patchResponse.body,
                    "Not found. Be sure that event exists and you are the organizer"
            ));
            return;
        }

        if (patchResponse.statusCode != 204) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.status(204);
    }

    private static void handleEventReaction(
            Context ctx,
            String sessionServiceUrl,
            String eventServiceUrl,
            int sessionTtlSeconds,
            String reactionType,
            boolean clearCookieOnUnauthorized
    ) {
        String requestSid = ServiceSupport.readValidSidFromCookie(ctx);
        if (requestSid == null) {
            if (clearCookieOnUnauthorized) {
                ServiceSupport.clearSessionCookie(ctx, "");
            }
            ctx.status(401);
            return;
        }

        InternalSessionInfoResponse session = getSessionInfo(sessionServiceUrl, requestSid);
        if (session == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (!session.exists() || ServiceSupport.isBlank(session.user_id())) {
            if (clearCookieOnUnauthorized) {
                ServiceSupport.clearSessionCookie(ctx, requestSid);
            }
            ctx.status(401);
            return;
        }

        touchSessionIfExists(sessionServiceUrl, requestSid);
        ServiceSupport.setSessionCookie(ctx, requestSid, sessionTtlSeconds);

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
        if (ServiceSupport.isBlank(eventId)) {
            ctx.status(404).json(new MessageResponse("Event not found"));
            return;
        }

        DownstreamResponse reactionResponse = postEmptyWithHeader(
                eventServiceUrl,
                "/internal/events/" + encodePathSegment(eventId) + "/" + reactionType,
                REACTION_USER_HEADER,
                session.user_id()
        );
        if (reactionResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (reactionResponse.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(reactionResponse.body, "Event not found"));
            return;
        }

        if (reactionResponse.statusCode != 204) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.status(204);
    }

    private static void handleGetEvent(Context ctx, String eventServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
        String path = withRawQuery("/internal/events/" + encodePathSegment(eventId), ctx.queryString());
        DownstreamResponse response = get(eventServiceUrl, path);
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(response.body, "Not found"));
            return;
        }

        if (response.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        EventResponse event = parseBody(response.body, EventResponse.class);
        if (event == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.json(event);
    }

    private static void handleListEvents(Context ctx, String eventServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String path = withRawQuery("/internal/events", ctx.queryString());
        DownstreamResponse response = get(eventServiceUrl, path);
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(response.body, "invalid \"limit\" field"));
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

    private static void handleListUsers(Context ctx, String userServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String path = withRawQuery("/internal/users", ctx.queryString());
        DownstreamResponse response = get(userServiceUrl, path);
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(response.body, "invalid \"limit\" field"));
            return;
        }

        if (response.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        UsersListResponse users = parseBody(response.body, UsersListResponse.class);
        if (users == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.json(users);
    }

    private static void handleGetUser(Context ctx, String userServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String userId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
        DownstreamResponse response = get(userServiceUrl, "/internal/users/" + encodePathSegment(userId));
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(response.body, "Not found"));
            return;
        }

        if (response.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        UserResponse user = parseBody(response.body, UserResponse.class);
        if (user == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.json(user);
    }

    private static void handleListUserEvents(Context ctx, String eventServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String userId = ServiceSupport.trimToEmpty(ctx.pathParam("id"));
        String path = withRawQuery("/internal/users/" + encodePathSegment(userId) + "/events", ctx.queryString());
        DownstreamResponse response = get(eventServiceUrl, path);
        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(response.body, "User not found"));
            return;
        }

        if (response.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(response.body, "invalid \"limit\" field"));
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

    private static void handleCreateReview(
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

        if (!session.exists() || ServiceSupport.isBlank(session.user_id())) {
            ctx.status(401);
            return;
        }

        touchSessionIfExists(sessionServiceUrl, requestSid);

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("event_id"));
        CreateReviewRequest request = ServiceSupport.readBody(ctx, CreateReviewRequest.class);
        if (request == null) {
            ctx.status(400).json(new MessageResponse("invalid \"comment\" field"));
            return;
        }

        if (ServiceSupport.isBlank(request.comment()) || request.comment().length() > 300) {
            ctx.status(400).json(new MessageResponse("invalid \"comment\" field"));
            return;
        }

        if (request.rating() == null || request.rating() < 1 || request.rating() > 5) {
            ctx.status(400).json(new MessageResponse("invalid \"rating\" field"));
            return;
        }

        DownstreamResponse createReviewResponse = postJsonWithHeader(
                eventServiceUrl,
                "/internal/events/" + encodePathSegment(eventId) + "/reviews",
                request,
                REACTION_USER_HEADER,
                session.user_id()
        );

        if (createReviewResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (createReviewResponse.statusCode == 409) {
            ctx.status(409).json(new MessageResponse("Already exists"));
            return;
        }

        if (createReviewResponse.statusCode == 404) {
            ctx.status(404).json(new MessageResponse("Event not found"));
            return;
        }

        if (createReviewResponse.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(createReviewResponse.body, "invalid \"comment\" field"));
            return;
        }

        if (createReviewResponse.statusCode != 201) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ReviewIdResponse review = parseBody(createReviewResponse.body, ReviewIdResponse.class);
        if (review == null || ServiceSupport.isBlank(review.id())) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.status(201).json(review);
    }

    private static void handleListReviews(Context ctx, String eventServiceUrl, int sessionTtlSeconds) {
        String sid = ServiceSupport.readValidSidFromCookie(ctx);
        ServiceSupport.maybeSetSessionCookie(ctx, sid, sessionTtlSeconds);

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("event_id"));
        String rawQuery = ServiceSupport.trimToEmpty(ctx.queryString());

        DownstreamResponse response = get(
                eventServiceUrl,
                withRawQuery("/internal/events/" + encodePathSegment(eventId) + "/reviews", rawQuery)
        );

        if (response == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (response.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(response.body, "invalid \"limit\" field"));
            return;
        }

        if (response.statusCode != 200) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ReviewsListResponse reviews = parseBody(response.body, ReviewsListResponse.class);
        if (reviews == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.json(reviews);
    }

    private static void handleUpdateReview(
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

        if (!session.exists() || ServiceSupport.isBlank(session.user_id())) {
            ctx.status(401);
            return;
        }

        touchSessionIfExists(sessionServiceUrl, requestSid);

        String eventId = ServiceSupport.trimToEmpty(ctx.pathParam("event_id"));
        String reviewId = ServiceSupport.trimToEmpty(ctx.pathParam("review_id"));

        DownstreamResponse patchResponse = patchRaw(
                eventServiceUrl,
                "/internal/events/" + encodePathSegment(eventId) + "/reviews/" + encodePathSegment(reviewId),
                ctx.body(),
                REACTION_USER_HEADER,
                session.user_id()
        );

        if (patchResponse == null) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        if (patchResponse.statusCode == 400) {
            ctx.status(400).json(readMessageOrFallback(patchResponse.body, "invalid request"));
            return;
        }

        if (patchResponse.statusCode == 404) {
            ctx.status(404).json(readMessageOrFallback(patchResponse.body, "Event not found"));
            return;
        }

        if (patchResponse.statusCode != 204) {
            ctx.status(502).json(new MessageResponse("external dependency failed"));
            return;
        }

        ctx.status(204);
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
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize POST JSON body: {}{}", baseUrl, path, e);
            return null;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream POST failed: {}{}", baseUrl, path, e);
            return null;
        }
    }

    private static DownstreamResponse postJsonWithHeader(
            String baseUrl,
            String path,
            Object payload,
            String headerName,
            String headerValue
    ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header(headerName, headerValue)
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload)))
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new DownstreamResponse(response.statusCode(), response.body());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize POST JSON body: {}{}", baseUrl, path, e);
            return null;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream POST failed: {}{}", baseUrl, path, e);
            return null;
        }
    }

    private static DownstreamResponse postEmptyWithHeader(
            String baseUrl,
            String path,
            String headerName,
            String headerValue
    ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .header(headerName, headerValue)
                    .POST(HttpRequest.BodyPublishers.noBody())
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

    private static DownstreamResponse patchRaw(
            String baseUrl,
            String path,
            String rawBody,
            String headerName,
            String headerValue
    ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeUrl(baseUrl, path)))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header(headerName, headerValue)
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(rawBody == null ? "" : rawBody))
                    .build();

            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new DownstreamResponse(response.statusCode(), response.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Downstream PATCH failed: {}{}", baseUrl, path, e);
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

    private static String withRawQuery(String path, String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return path;
        }
        return path + "?" + rawQuery;
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    private static MessageResponse readMessageOrFallback(String body, String fallbackMessage) {
        MessageResponse parsed = parseBody(body, MessageResponse.class);
        if (parsed == null || ServiceSupport.isBlank(parsed.message())) {
            return new MessageResponse(fallbackMessage);
        }
        return parsed;
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