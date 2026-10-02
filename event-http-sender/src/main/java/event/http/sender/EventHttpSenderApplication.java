package event.http.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "event.http.sender")
public class EventHttpSenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventHttpSenderApplication.class, args);
    }
}
