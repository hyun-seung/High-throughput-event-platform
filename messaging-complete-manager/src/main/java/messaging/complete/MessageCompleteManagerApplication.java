package messaging.complete;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.complete")
public class MessageCompleteManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageCompleteManagerApplication.class, args);
    }
}
