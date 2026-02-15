package healthcheck;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String PORT_ENV = "APP_PORT";
    private static final int DEFAULT_PORT = 8080;

    private record HealthResponse(String status) {
    }

    public static void main(String[] args) {
        int port = envPort(PORT_ENV, DEFAULT_PORT);

        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        app.start(port);
        log.info("Service started on port {}", port);
    }

    private static int envPort(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int port = Integer.parseInt(raw.trim());
            return (port >= 1 && port <= 65535) ? port : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
