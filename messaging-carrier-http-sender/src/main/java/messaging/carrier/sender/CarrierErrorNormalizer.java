package messaging.carrier.sender;

import messaging.common.messages.CarrierHttpFailureCodes;
import messaging.common.messages.PrimaryHttpFailureCodes;

import java.util.Objects;
import java.util.Set;

/** Maps carrier-local codes to the messaging service's shared 6xxxx namespace. */
public final class CarrierErrorNormalizer {
    private final Set<String> notOurCarrierCodes;
    private final Set<String> tpsExceededCodes;

    public CarrierErrorNormalizer(Set<String> notOurCarrierCodes, Set<String> tpsExceededCodes) {
        this.notOurCarrierCodes = Set.copyOf(Objects.requireNonNull(notOurCarrierCodes));
        this.tpsExceededCodes = Set.copyOf(Objects.requireNonNull(tpsExceededCodes));
        if (!this.notOurCarrierCodes.stream().allMatch(CarrierErrorNormalizer::fiveDigits)
                || !this.tpsExceededCodes.stream().allMatch(CarrierErrorNormalizer::fiveDigits)
                || this.notOurCarrierCodes.stream().anyMatch(this.tpsExceededCodes::contains)) {
            throw new IllegalArgumentException("Invalid carrier error code mapping");
        }
    }

    public int normalize(String providerCode) {
        if (providerCode != null && notOurCarrierCodes.contains(providerCode)) {
            return PrimaryHttpFailureCodes.NOT_OUR_CARRIER;
        }
        if (providerCode != null && tpsExceededCodes.contains(providerCode)) {
            return PrimaryHttpFailureCodes.TPS_EXCEEDED;
        }
        if (providerCode != null && fiveDigits(providerCode) && providerCode.charAt(0) == '6') {
            return Integer.parseInt(providerCode);
        }
        return CarrierHttpFailureCodes.UNMAPPED_PROVIDER_FAILURE;
    }

    private static boolean fiveDigits(String code) { return code != null && code.matches("[0-9]{5}"); }
}
