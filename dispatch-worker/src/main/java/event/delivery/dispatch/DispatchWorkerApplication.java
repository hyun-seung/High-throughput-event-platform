package event.delivery.dispatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class DispatchWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DispatchWorkerApplication.class, args);
        log.info("Dispatch Worker started");
    }
}
