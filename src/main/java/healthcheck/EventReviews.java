package healthcheck;

public record EventReviews(int count, double rating) {
    public static EventReviews empty() {
        return new EventReviews(0, 0.0);
    }

    public boolean hasAny() {
        return count > 0;
    }
}