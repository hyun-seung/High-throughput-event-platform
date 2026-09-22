package event.common.delivery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class DeliveryPayloads {

    private DeliveryPayloads() {
    }

    public static Map<String, Object> canonicalize(Map<String, Object> payload) {
        return Collections.unmodifiableMap(canonicalMap(payload));
    }

    private static Map<String, Object> canonicalMap(Map<String, ?> source) {
        Map<String, Object> sorted = new TreeMap<>();
        source.forEach((key, value) -> sorted.put(key, canonicalValue(value)));
        return new LinkedHashMap<>(sorted);
    }

    private static Object canonicalValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> stringKeyMap = new LinkedHashMap<>();
            map.forEach((key, nestedValue) -> stringKeyMap.put(String.valueOf(key), nestedValue));
            return Collections.unmodifiableMap(canonicalMap(stringKeyMap));
        }

        if (value instanceof List<?> list) {
            List<Object> canonical = new ArrayList<>(list.size());
            list.forEach(element -> canonical.add(canonicalValue(element)));
            return Collections.unmodifiableList(canonical);
        }

        return value;
    }
}
