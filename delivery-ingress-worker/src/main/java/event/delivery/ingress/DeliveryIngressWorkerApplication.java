package event.delivery.ingress;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class DeliveryIngressWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryIngressWorkerApplication.class, args);
        log.info("Delivery Ingress Worker started");
    }
}
