package messaging.reference.cache;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.reference.cache")
public class MessageReferenceCacheApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageReferenceCacheApplication.class, args);
    }
}
