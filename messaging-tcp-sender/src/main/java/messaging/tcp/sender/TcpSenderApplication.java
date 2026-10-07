package messaging.tcp.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "messaging.tcp.sender")
public class TcpSenderApplication {
    public static void main(String[] args) { SpringApplication.run(TcpSenderApplication.class, args); }
}
