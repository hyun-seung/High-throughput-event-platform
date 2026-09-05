package external.api.simulator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class ExternalApiSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExternalApiSimulatorApplication.class, args);

        log.info("\n" +
                "=====================================\n" +
                "  External Api Simulator             \n" +
                "=====================================\n" );
    }
}