package event.processing.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class EventProcessingWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventProcessingWorkerApplication.class, args);

        log.info("\n" +
                "=====================================\n" +
                "  Event Processing Worker            \n" +
                "=====================================\n" );
    }
}