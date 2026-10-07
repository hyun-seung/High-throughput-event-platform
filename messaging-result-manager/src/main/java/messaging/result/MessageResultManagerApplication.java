package messaging.result;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication(scanBasePackages = "messaging.result")
public class MessageResultManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageResultManagerApplication.class, args);
    }
}
