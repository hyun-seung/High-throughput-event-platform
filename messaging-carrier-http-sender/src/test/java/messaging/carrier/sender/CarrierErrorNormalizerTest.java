package messaging.carrier.sender;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CarrierErrorNormalizerTest {
    @Test
    void mapsCarrierLocalCodesAndPreservesOtherSixtyThousandBandCodes() {
        var normalizer = new CarrierErrorNormalizer(Set.of("41001"), Set.of("42002"));
        assertEquals(66001, normalizer.normalize("41001"));
        assertEquals(66002, normalizer.normalize("42002"));
        assertEquals(61005, normalizer.normalize("61005"));
        assertEquals(66999, normalizer.normalize("49000"));
        assertEquals(66999, normalizer.normalize(null));
    }

    @Test
    void rejectsAmbiguousMappings() {
        assertThrows(IllegalArgumentException.class,
                () -> new CarrierErrorNormalizer(Set.of("41001"), Set.of("41001")));
    }
}
