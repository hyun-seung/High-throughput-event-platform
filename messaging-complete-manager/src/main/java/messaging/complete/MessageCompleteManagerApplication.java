package messaging.complete;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "messaging.complete")
@EnableScheduling
public class MessageCompleteManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageCompleteManagerApplication.class, args);
    }
}
