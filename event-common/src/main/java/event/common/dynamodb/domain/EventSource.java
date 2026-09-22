package event.common.dynamodb.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventSource {

    DIRECT_API("Event API 직접 인입"),
    EXTERNAL_DYNAMODB("외부 DynamoDB 직접 인입");

    private final String desc;
}