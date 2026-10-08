package messaging.api.messages;

import messaging.api.security.AuthenticatedUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class MessageAdmissionResponseTest {
    private final MessageReceiveService service = mock(MessageReceiveService.class);
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new MessageReceiveController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setCustomArgumentResolvers(new HandlerMethodArgumentResolver() {
                    @Override public boolean supportsParameter(MethodParameter parameter) {
                        return parameter.getParameterType() == AuthenticatedUser.class;
                    }
                    @Override public Object resolveArgument(MethodParameter parameter,
                            ModelAndViewContainer container, NativeWebRequest request,
                            WebDataBinderFactory binderFactory) {
                        return new AuthenticatedUser(42L, "client");
                    }
                }).build();
    }

    @ParameterizedTest
    @CsvSource({"BAD_REQUEST,50000", "CONFLICT,50003", "TOO_MANY_REQUESTS,30000", "SERVICE_UNAVAILABLE,50004"})
    void admissionFailuresReturnFiveDigitServiceCodes(HttpStatus httpStatus, int errorCode) throws Exception {
        when(service.receive(eq(42L), any())).thenThrow(new MessageAdmissionException(httpStatus, "Rejected"));

        mvc.perform(post("/api/v1/messages").contentType(MediaType.APPLICATION_JSON).content("""
                {"messageId":"customer-1","recipientNumber":"01012345678",
                 "messageCategory":"GENERAL","payload":{"text":"hello"}}
                """))
                .andExpect(status().is(httpStatus.value()))
                .andExpect(jsonPath("$.status").value(httpStatus.value()))
                .andExpect(jsonPath("$.data.code").value(errorCode))
                .andExpect(jsonPath("$.data.message").value("Rejected"));
    }

    @Test
    void malformedJsonKeepsTheExistingBodyErrorContract() throws Exception {
        mvc.perform(post("/api/v1/messages").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.data.code").value(50002));
        verifyNoInteractions(service);
    }
}
