package healthcheck;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String PORT_ENV = "APP_PORT";

    private record HealthResponse(String status) {
    }

    public static void main(String[] args) {
        int port = requirePort(PORT_ENV);

        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        app.start(port);
        log.info("Service started on port {}", port);
    }

    private static int requirePort(String name) {
        String raw = System.getenv(name);

        if (raw == null || raw.isBlank()) {
            log.error("Environment variable {} is required (example: {}=8080).", name, name);
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(raw.trim());
            if (port < 1 || port > 65535) {
                log.error("Invalid {}={}.", name, raw);
                System.exit(1);
            }
            return port;
        } catch (NumberFormatException e) {
            log.error("Invalid {}={}.", name, raw);
            System.exit(1);
            return -1;
        }
    }
}