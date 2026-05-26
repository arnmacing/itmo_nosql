package healthcheck;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ReviewStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReviewStore.class);

    private static final String TABLE_NAME = "event_reviews";
    private static final String CACHE_KEY_PATTERN = "event:%s:reviews";
    private static final String CACHE_FIELD_COUNT = "count";
    private static final String CACHE_FIELD_RATING = "rating";

    private final CqlSession cassandraSession;
    private final PreparedStatement insertReviewStatement;
    private final PreparedStatement selectReviewByEventAndUserStatement;
    private final PreparedStatement selectReviewsByEventStatement;
    private final PreparedStatement selectReviewByIdStatement;
    private final PreparedStatement updateReviewStatement;
    private final PreparedStatement selectAllReviewsByEventStatement;
    private final ConsistencyLevel cassandraConsistency;
    private final JedisPooled reviewsCache;
    private final int reviewsTtlSeconds;

    public ReviewStore(
            CqlSession cassandraSession,
            String cassandraKeyspace,
            ConsistencyLevel cassandraConsistency,
            JedisPooled reviewsCache,
            int reviewsTtlSeconds
    ) {
        this.cassandraSession = cassandraSession;
        this.cassandraConsistency = cassandraConsistency;
        this.reviewsCache = reviewsCache;
        this.reviewsTtlSeconds = reviewsTtlSeconds;

        String reviewsTable = ensureReviewsSchema(cassandraKeyspace);
        this.insertReviewStatement = cassandraSession.prepare(
                "INSERT INTO " + reviewsTable + " (id, event_id, created_by, rating, comment, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
        );
        this.selectReviewByEventAndUserStatement = cassandraSession.prepare(
                "SELECT id FROM " + reviewsTable + " WHERE event_id = ? AND created_by = ? ALLOW FILTERING"
        );
        this.selectReviewsByEventStatement = cassandraSession.prepare(
                "SELECT id, event_id, created_by, rating, comment, created_at, updated_at FROM " + reviewsTable + " WHERE event_id = ? ORDER BY created_at DESC"
        );
        this.selectReviewByIdStatement = cassandraSession.prepare(
                "SELECT id, event_id, created_by, rating, comment, created_at, updated_at FROM " + reviewsTable + " WHERE event_id = ? AND id = ? ALLOW FILTERING"
        );
        this.updateReviewStatement = cassandraSession.prepare(
                "UPDATE " + reviewsTable + " SET rating = ?, comment = ?, updated_at = ? WHERE event_id = ? AND created_at = ? AND id = ? IF created_by = ?"
        );
        this.selectAllReviewsByEventStatement = cassandraSession.prepare(
                "SELECT rating FROM " + reviewsTable + " WHERE event_id = ?"
        );
    }

    private String ensureReviewsSchema(String keyspaceName) {
        String table = keyspaceName + "." + TABLE_NAME;

        cassandraSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
                        + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
        );
        cassandraSession.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                        + "id uuid, "
                        + "event_id text, "
                        + "rating tinyint, "
                        + "comment text, "
                        + "created_at timestamp, "
                        + "created_by text, "
                        + "updated_at timestamp, "
                        + "PRIMARY KEY ((event_id), created_at, id)"
                        + ") WITH CLUSTERING ORDER BY (created_at DESC, id ASC)"
        );
        cassandraSession.execute(
                "CREATE INDEX IF NOT EXISTS event_reviews_created_by_idx ON " + table + " (created_by)"
        );

        return table;
    }

    public static class ReviewCreationResult {
        private final boolean created;
        private final boolean alreadyExists;
        private final String reviewId;

        private ReviewCreationResult(boolean created, boolean alreadyExists, String reviewId) {
            this.created = created;
            this.alreadyExists = alreadyExists;
            this.reviewId = reviewId;
        }

        public static ReviewCreationResult success(String reviewId) {
            return new ReviewCreationResult(true, false, reviewId);
        }

        public static ReviewCreationResult duplicate() {
            return new ReviewCreationResult(false, true, null);
        }

        public static ReviewCreationResult failure() {
            return new ReviewCreationResult(false, false, null);
        }

        public boolean created() {
            return created;
        }

        public boolean alreadyExists() {
            return alreadyExists;
        }

        public String reviewId() {
            return reviewId;
        }
    }

    public ReviewCreationResult createReview(String eventId, String userId, String comment, int rating) {
        if (ServiceSupport.isBlank(eventId) || ServiceSupport.isBlank(userId)) {
            return ReviewCreationResult.failure();
        }

        BoundStatement checkStatement = selectReviewByEventAndUserStatement
                .bind(eventId, userId)
                .setConsistencyLevel(cassandraConsistency);
        ResultSet existingReviews = cassandraSession.execute(checkStatement);
        if (existingReviews.one() != null) {
            return ReviewCreationResult.duplicate();
        }

        UUID reviewId = UUID.randomUUID();
        Instant now = Instant.now();
        BoundStatement statement = insertReviewStatement
                .bind(reviewId, eventId, userId, (byte) rating, comment, now, now)
                .setConsistencyLevel(cassandraConsistency);
        cassandraSession.execute(statement);

        return ReviewCreationResult.success(reviewId.toString());
    }

    public List<ReviewResponse> listReviews(String eventId, Integer limit, int offset) {
        if (ServiceSupport.isBlank(eventId)) {
            return List.of();
        }

        BoundStatement statement = selectReviewsByEventStatement
                .bind(eventId)
                .setConsistencyLevel(cassandraConsistency);
        ResultSet result = cassandraSession.execute(statement);

        List<ReviewResponse> allReviews = new ArrayList<>();
        for (Row row : result) {
            allReviews.add(mapReview(row));
        }

        int start = Math.min(offset, allReviews.size());
        int end = allReviews.size();
        if (limit != null && limit >= 0) {
            end = Math.min(start + limit, allReviews.size());
        }

        return new ArrayList<>(allReviews.subList(start, end));
    }

    public boolean updateReview(String eventId, String reviewId, String userId, String comment, Integer rating) {
        if (ServiceSupport.isBlank(eventId) || ServiceSupport.isBlank(reviewId) || ServiceSupport.isBlank(userId)) {
            return false;
        }

        UUID reviewUuid;
        try {
            reviewUuid = UUID.fromString(reviewId);
        } catch (IllegalArgumentException e) {
            return false;
        }

        BoundStatement selectStatement = selectReviewByIdStatement
                .bind(eventId, reviewUuid)
                .setConsistencyLevel(cassandraConsistency);
        Row existingReview = cassandraSession.execute(selectStatement).one();
        if (existingReview == null) {
            return false;
        }

        String existingCreatedBy = existingReview.getString("created_by");
        if (!userId.equals(existingCreatedBy)) {
            return false;
        }

        byte newRating = rating != null ? rating.byteValue() : existingReview.getByte("rating");
        String newComment = comment != null ? comment.trim() : existingReview.getString("comment");
        Instant createdAt = existingReview.getInstant("created_at");
        if (createdAt == null) {
            return false;
        }

        BoundStatement updateStatement = updateReviewStatement
                .bind(newRating, newComment, Instant.now(), eventId, createdAt, reviewUuid, userId)
                .setConsistencyLevel(cassandraConsistency);
        cassandraSession.execute(updateStatement);

        return true;
    }

    private ReviewResponse mapReview(Row row) {
        UUID id = row.getUuid("id");
        String eventId = row.getString("event_id");
        String createdBy = row.getString("created_by");
        Byte ratingByte = row.getByte("rating");
        int rating = ratingByte != null ? ratingByte : 0;
        String comment = row.getString("comment");
        Instant createdAt = row.getInstant("created_at");
        Instant updatedAt = row.getInstant("updated_at");

        return new ReviewResponse(
                id.toString(),
                eventId,
                comment,
                createdAt != null ? OffsetDateTime.ofInstant(createdAt, java.time.ZoneOffset.UTC).toString() : "",
                createdBy,
                rating,
                updatedAt != null ? OffsetDateTime.ofInstant(updatedAt, java.time.ZoneOffset.UTC).toString() : ""
        );
    }

    public EventReviews getReviewsSummary(List<String> eventIds) {
        if (eventIds.isEmpty()) {
            return EventReviews.empty();
        }

        int totalCount = 0;
        double totalRating = 0.0;

        for (String eventId : eventIds) {
            BoundStatement statement = selectAllReviewsByEventStatement
                    .bind(eventId)
                    .setConsistencyLevel(cassandraConsistency);
            ResultSet result = cassandraSession.execute(statement);

            for (Row row : result) {
                Byte ratingByte = row.getByte("rating");
                if (ratingByte != null) {
                    totalCount++;
                    totalRating += ratingByte;
                }
            }
        }

        if (totalCount == 0) {
            return EventReviews.empty();
        }

        double avgRating = Math.round(totalRating / totalCount * 10.0) / 10.0;
        return new EventReviews(totalCount, avgRating);
    }

    public EventReviews getReviewsFromCache(String eventTitle) {
        if (ServiceSupport.isBlank(eventTitle)) {
            return null;
        }

        String cacheKey = titleReviewsCacheKey(eventTitle);
        Map<String, String> values = reviewsCache.hgetAll(cacheKey);
        if (values == null || values.isEmpty()) {
            return null;
        }

        Integer count = parseNonNegativeInt(values.get(CACHE_FIELD_COUNT));
        Double rating = parseDouble(values.get(CACHE_FIELD_RATING));
        if (count == null || rating == null) {
            reviewsCache.del(cacheKey);
            return null;
        }

        return new EventReviews(count, rating);
    }

    public void storeReviewsInCache(String eventTitle, EventReviews reviews) {
        if (ServiceSupport.isBlank(eventTitle)) {
            return;
        }

        String cacheKey = titleReviewsCacheKey(eventTitle);
        try {
            Map<String, String> values = Map.of(
                    CACHE_FIELD_COUNT, String.valueOf(reviews.count()),
                    CACHE_FIELD_RATING, String.valueOf(reviews.rating())
            );
            reviewsCache.hset(cacheKey, values);
            reviewsCache.expire(cacheKey, reviewsTtlSeconds);
        } catch (Exception e) {
            log.warn("Failed to write reviews cache for key {}", cacheKey, e);
        }
    }

    public void invalidateCache(String eventTitle) {
        if (ServiceSupport.isBlank(eventTitle)) {
            return;
        }
        reviewsCache.del(titleReviewsCacheKey(eventTitle));
    }

    private static String titleReviewsCacheKey(String title) {
        return CACHE_KEY_PATTERN.formatted(md5Hex(title));
    }

    private static String md5Hex(String value) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is not available", e);
        }
    }

    private static Integer parseNonNegativeInt(String value) {
        if (ServiceSupport.isBlank(value)) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed < 0 ? null : parsed;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double parseDouble(String value) {
        if (ServiceSupport.isBlank(value)) {
            return null;
        }
        try {
            double parsed = Double.parseDouble(value.trim());
            return parsed < 0 ? null : parsed;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void close() {
        reviewsCache.close();
    }
}