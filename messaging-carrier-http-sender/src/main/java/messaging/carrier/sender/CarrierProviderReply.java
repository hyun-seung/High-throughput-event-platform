package messaging.carrier.sender;

/** HTTP 200, explicit provider failure, or no response within the configured timeout. */
public sealed interface CarrierProviderReply permits CarrierProviderReply.Accepted,
        CarrierProviderReply.Failed, CarrierProviderReply.TimedOut {
    record Accepted() implements CarrierProviderReply { }
    record Failed(int httpStatus, String providerStatus, String code, String message) implements CarrierProviderReply { }
    record TimedOut() implements CarrierProviderReply { }
}
