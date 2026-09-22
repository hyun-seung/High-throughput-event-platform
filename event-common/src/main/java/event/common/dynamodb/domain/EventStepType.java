package event.common.dynamodb.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventStepType {

    INGRESS("INGRESS", "인입"),
    PREPARE("PREPARE", "발송 데이터 준비"),
    SEND_1("SEND#1", "1차 외부 발송"),
    WEBHOOK_1("WEBHOOK#1", "1차 발송 결과 수신"),
    SEND_2("SEND#2", "2차 외부 발송"),
    CORP_WEBHOOK("CORP_WEBHOOK", "기업 Webhook"),
    ARCHIVE("ARCHIVE", "최종 이력 저장");

    private static final String PREFIX = "STEP#";

    private final String key;
    private final String desc;

    public String sortKey() {
        return PREFIX + key;
    }
}
