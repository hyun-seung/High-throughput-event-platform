package messaging.carrier.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.carrier.sender")
public class CarrierHttpSenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(CarrierHttpSenderApplication.class, args);
    }
}
