package messaging.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class MessageReceiveApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(MessageReceiveApiApplication.class, args);
        log.info("Message receive API started");
    }
}
