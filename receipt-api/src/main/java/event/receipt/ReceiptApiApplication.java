package event.receipt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ReceiptApiApplication {
    public static void main(String[] args) { SpringApplication.run(ReceiptApiApplication.class, args); }
}
