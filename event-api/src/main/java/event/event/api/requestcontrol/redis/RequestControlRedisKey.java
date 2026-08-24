package event.event.api.requestcontrol.redis;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class RequestControlRedisKey {

    private static final String PREFIX = "request-control";
    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");

    private RequestControlRedisKey() {
    }

    public static String policy(Long userId) {
        validateUserId(userId);
        return PREFIX + ":{user:" + userId + "}:policy";
    }

    public static String bucket(Long userId) {
        validateUserId(userId);
        return PREFIX + ":{user:" + userId + "}:bucket";
    }

    public static String quota(Long userId, YearMonth yearMonth) {
        validateUserId(userId);
        Objects.requireNonNull(yearMonth, "yearMonth must not be null.");
        return PREFIX + ":{user:" + userId + "}:quota:" + yearMonth.format(YEAR_MONTH_FORMATTER);
    }

    private static void validateUserId(Long userId) {
        if (Objects.isNull(userId) || userId <= 0) {
            throw new IllegalArgumentException("userId must be greater than 0.");
        }
    }
}