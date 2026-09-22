package event.common.dynamodb.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum PublishStatus {

    PENDING("발행 대기"),
    PUBLISHED("발행 완료"),
    RETRY_REQUIRED("재발행 필요"),
    RECOVERING("복구 처리 중");

    private final String desc;
}
