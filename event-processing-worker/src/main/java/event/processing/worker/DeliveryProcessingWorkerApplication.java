package event.processing.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class DeliveryProcessingWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryProcessingWorkerApplication.class, args);
        log.info("Delivery Processing Worker started");
    }
}
