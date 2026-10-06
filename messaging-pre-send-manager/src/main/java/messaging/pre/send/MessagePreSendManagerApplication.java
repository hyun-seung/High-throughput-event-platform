package messaging.pre.send;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.pre.send")
public class MessagePreSendManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessagePreSendManagerApplication.class, args);
    }
}
