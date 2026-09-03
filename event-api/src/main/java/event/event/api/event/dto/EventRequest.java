package event.event.api.event.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Map;

public record EventRequest(
        @NotBlank
        @Size(max = 100)
        String eventType,

        @NotNull
        Map<String, Object> payload
) {
}