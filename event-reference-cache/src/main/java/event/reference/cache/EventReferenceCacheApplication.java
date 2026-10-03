package event.reference.cache;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "event.reference.cache")
public class EventReferenceCacheApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventReferenceCacheApplication.class, args);
    }
}
