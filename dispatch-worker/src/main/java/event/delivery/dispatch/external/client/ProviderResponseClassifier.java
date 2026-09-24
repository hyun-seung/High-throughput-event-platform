package event.delivery.dispatch.external.client;

import event.delivery.dispatch.external.dto.ProviderDispatchResponse;

import static event.delivery.dispatch.external.client.ProviderFailureException.Kind.*;

final class ProviderResponseClassifier {
    private ProviderResponseClassifier() { }

    static ProviderDispatchResponse classify(String deliveryId, int status, ProviderDispatchResponse response) {
        if (response == null || !deliveryId.equals(response.deliveryId()) || response.accepted() == null
                || response.processedAt() == null) {
            throw new ProviderFailureException(INVALID_RESPONSE);
        }
        if (response.accepted()) {
            // Accept the original simulator's three-field success response during transition.
            if (status >= 200 && status < 300 && (response.code() == null || "ACCEPTED".equals(response.code()))) {
                return response;
            }
            throw new ProviderFailureException(INVALID_RESPONSE);
        }
        if (status < 200 || (status >= 300 && status < 400)) {
            throw new ProviderFailureException(INVALID_RESPONSE);
        }
        if (response.code() == null) {
            throw new ProviderFailureException(status >= 400 ? HTTP_ERROR : INVALID_RESPONSE);
        }
        throw new ProviderFailureException(switch (response.code()) {
            case "RETRY_1S" -> RETRY_1S;
            case "RETRY_10S" -> RETRY_10S;
            case "FALLBACK" -> FALLBACK_REQUIRED;
            case "REJECTED" -> PERMANENT_REJECTION;
            default -> INVALID_RESPONSE;
        });
    }
}
