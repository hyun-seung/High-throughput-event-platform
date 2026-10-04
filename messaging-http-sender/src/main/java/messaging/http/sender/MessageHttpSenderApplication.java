package messaging.http.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.http.sender")
public class MessageHttpSenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageHttpSenderApplication.class, args);
    }
}
