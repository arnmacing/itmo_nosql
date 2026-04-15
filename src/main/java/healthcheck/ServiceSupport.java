package healthcheck;

import io.javalin.http.Context;
import org.slf4j.Logger;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.regex.Pattern;

final class ServiceSupport {

    static final String SESSION_COOKIE_NAME = "X-Session-Id";
    static final Pattern SID_PATTERN = Pattern.compile("^[0-9a-f]{32}$");
    static final DateTimeFormatter DATE_FILTER_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE
            .withResolverStyle(ResolverStyle.STRICT);

    private ServiceSupport() {
    }

    static String requireNonBlankEnv(String name, Logger log) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            log.error("Environment variable {} is required.", name);
            System.exit(1);
        }
        return raw.trim();
    }

    static int requirePortEnv(String name, Logger log) {
        String raw = requireNonBlankEnv(name, log);
        try {
            int value = Integer.parseInt(raw);
            if (value < 1 || value > 65535) {
                log.error("Invalid {}={}", name, raw);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            log.error("Invalid {}={}", name, raw);
            System.exit(1);
            return -1;
        }
    }

    static int requirePositiveIntEnv(String name, Logger log) {
        String raw = requireNonBlankEnv(name, log);
        try {
            int value = Integer.parseInt(raw);
            if (value < 1) {
                log.error("Invalid {}={}", name, raw);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            log.error("Invalid {}={}", name, raw);
            System.exit(1);
            return -1;
        }
    }

    static int requireNonNegativeIntEnv(String name, Logger log) {
        String raw = requireNonBlankEnv(name, log);
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) {
                log.error("Invalid {}={}", name, raw);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            log.error("Invalid {}={}", name, raw);
            System.exit(1);
            return -1;
        }
    }

    static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    static boolean isValidSid(String sid) {
        return sid != null && SID_PATTERN.matcher(sid).matches();
    }

    static String readValidSidFromCookie(Context ctx) {
        String rawSid = ctx.cookie(SESSION_COOKIE_NAME);
        if (rawSid == null) {
            return null;
        }
        String sid = rawSid.trim();
        return isValidSid(sid) ? sid : null;
    }

    static void setSessionCookie(Context ctx, String sid, int ttlSeconds) {
        ctx.header("Set-Cookie", SESSION_COOKIE_NAME + "=" + sid + "; HttpOnly; Path=/; Max-Age=" + ttlSeconds);
    }

    static void maybeSetSessionCookie(Context ctx, String sid, int ttlSeconds) {
        if (sid != null) {
            setSessionCookie(ctx, sid, ttlSeconds);
        }
    }

    static void clearSessionCookie(Context ctx, String sid) {
        String value = sid == null ? "" : sid;
        ctx.header("Set-Cookie", SESSION_COOKIE_NAME + "=" + value + "; HttpOnly; Path=/; Max-Age=0");
    }

    static <T> T readBody(Context ctx, Class<T> clazz) {
        try {
            return ctx.bodyAsClass(clazz);
        } catch (Exception e) {
            return null;
        }
    }

    static boolean isValidRfc3339(String value) {
        try {
            OffsetDateTime.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    static Integer parseUnsignedQueryInt(String value) {
        if (value == null) {
            return null;
        }

        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid query integer");
        }

        if (parsed < 0 || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid query integer");
        }
        return (int) parsed;
    }

    static Integer parseUnsignedQueryInt(String value, String fieldName) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName);
        }

        long parsed;
        try {
            parsed = Long.parseLong(trimmed);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(fieldName);
        }

        if (parsed < 0 || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(fieldName);
        }

        return (int) parsed;
    }

    static String readOptionalQueryString(Context ctx, String name) {
        String value = ctx.queryParam(name);
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name);
        }

        return trimmed;
    }

    static LocalDate parseDateQuery(String value, String fieldName) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName);
        }

        try {
            return LocalDate.parse(trimmed, DATE_FILTER_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(fieldName);
        }
    }

    static String defaultString(String value) {
        return value == null ? "" : value;
    }
}