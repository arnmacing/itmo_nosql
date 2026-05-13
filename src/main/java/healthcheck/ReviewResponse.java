package healthcheck;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReviewResponse(
        String id,
        String event_id,
        String comment,
        String created_at,
        String created_by,
        int rating,
        String updated_at
) {}