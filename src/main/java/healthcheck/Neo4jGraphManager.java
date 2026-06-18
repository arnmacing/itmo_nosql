package healthcheck;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Neo4jGraphManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Neo4jGraphManager.class);
    private final Driver driver;

    public Neo4jGraphManager(String neo4jUrl, String neo4jUser, String neo4jPassword) {
        if (ServiceSupport.isBlank(neo4jPassword)) {
            this.driver = GraphDatabase.driver(neo4jUrl);
        } else {
            this.driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(neo4jUser, neo4jPassword));
        }
        ensureIndexes();
    }

    private void ensureIndexes() {
        try (Session session = driver.session(SessionConfig.defaultConfig())) {
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE");
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE");
            session.run("CREATE INDEX IF NOT EXISTS FOR (e:Event) ON (e.title)");
        } catch (Exception e) {
            log.warn("Failed to create Neo4j indexes", e);
        }
    }

    public void createUser(String userId) {
        if (ServiceSupport.isBlank(userId)) {
            return;
        }
        try (Session session = driver.session(SessionConfig.defaultConfig())) {
            session.run(
                    "MERGE (u:User {id: $userId})",
                    Map.of("userId", userId)
            );
        } catch (Exception e) {
            log.warn("Failed to create user node in Neo4j for userId={}", userId, e);
        }
    }

    public void createEvent(String eventId, String title) {
        if (ServiceSupport.isBlank(eventId)) {
            return;
        }
        try (Session session = driver.session(SessionConfig.defaultConfig())) {
            session.run(
                    "MERGE (e:Event {id: $eventId}) SET e.title = $title",
                    Map.of("eventId", eventId, "title", ServiceSupport.defaultString(title))
            );
        } catch (Exception e) {
            log.warn("Failed to create event node in Neo4j for eventId={}", eventId, e);
        }
    }

    public void createLike(String userId, String eventId) {
        if (ServiceSupport.isBlank(userId) || ServiceSupport.isBlank(eventId)) {
            return;
        }
        try (Session session = driver.session(SessionConfig.defaultConfig())) {
            session.run(
                    "MERGE (u:User {id: $userId}) " +
                            "MERGE (e:Event {id: $eventId}) " +
                            "MERGE (u)-[:LIKED]->(e)",
                    Map.of("userId", userId, "eventId", eventId)
            );
        } catch (Exception e) {
            log.warn("Failed to create LIKED relationship in Neo4j for userId={}, eventId={}", userId, eventId, e);
        }
    }

    public List<String> getRecommendedEventIds(String userId) {
        if (ServiceSupport.isBlank(userId)) {
            return List.of();
        }

        try (Session session = driver.session(SessionConfig.defaultConfig())) {
            String query = """
                    MATCH (user:User {id: $userId})-[:LIKED]->(likedEvent:Event)
                    MATCH (otherUser:User)-[:LIKED]->(likedEvent)
                    WHERE otherUser.id <> user.id
                    MATCH (otherUser)-[:LIKED]->(recommendedEvent:Event)
                    WHERE NOT (user)-[:LIKED]->(recommendedEvent)
                    RETURN DISTINCT recommendedEvent.id AS eventId,
                           count(DISTINCT otherUser) AS popularity
                    ORDER BY popularity DESC, eventId
                    """;

            Result result = session.run(query, Map.of("userId", userId));
            List<String> eventIds = new ArrayList<>();
            while (result.hasNext()) {
                Record record = result.next();
                String eventId = record.get("eventId").asString();
                if (!ServiceSupport.isBlank(eventId)) {
                    eventIds.add(eventId);
                }
            }
            return eventIds;
        } catch (Exception e) {
            log.warn("Failed to get recommendations from Neo4j for userId={}", userId, e);
            return List.of();
        }
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }
}
