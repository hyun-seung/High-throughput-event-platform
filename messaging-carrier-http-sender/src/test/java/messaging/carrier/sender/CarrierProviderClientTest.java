package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class CarrierProviderClientTest {
    private final HttpSendCommand command = new HttpSendCommand("attempt-1", HttpCarrier.KT, 1,
            Instant.parse("2026-10-07T03:00:00Z"), new HttpProviderRequest("a".repeat(32), 42L,
            "GENERAL", "01012345678", Map.of("text", "hello"), Instant.parse("2026-10-07T00:00:00Z")));

    @Test
    void parsesTheProvidedNon200ErrorEnvelope() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(once(), requestTo("http://kt.test/api/v1/messages"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().json("{\"clientMsgId\":\"" + command.request().clientMsgId() + "\"}"))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST)
                        .body("{\"status\":\"4xx\",\"error\":{\"code\":\"41001\",\"message\":\"not ours\"}}"));
        var provider = new CarrierProviderClient(builder.baseUrl("http://kt.test").build(),
                JsonMapper.builder().build(), "/api/v1/messages");

        assertEquals(new CarrierProviderReply.Failed(400, "4xx", "41001", "not ours"),
                provider.send(command));
        server.verify();
    }

    @Test
    void http200NeedsNoResultBody() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://kt.test/api/v1/messages"))
                .andRespond(withStatus(HttpStatus.OK));
        var provider = new CarrierProviderClient(builder.baseUrl("http://kt.test").build(),
                JsonMapper.builder().build(), "/api/v1/messages");

        assertInstanceOf(CarrierProviderReply.Accepted.class, provider.send(command));
        server.verify();
    }
}
