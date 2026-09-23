package event.delivery.ingress.kafka.config;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<Object, Object> dltProducerFactory(KafkaProperties kafkaProperties) {
        Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();

        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(Object.class, new JacksonJsonSerializer<>());

        DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(serializers, true);
        DelegatingByTypeSerializer keySerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(), String.class, new StringSerializer()));

        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                keySerializer,
                valueSerializer
        );
    }

    @Bean
    public KafkaTemplate<Object, Object> dltKafkaTemplate(
            ProducerFactory<Object, Object> dltProducerFactory
    ) {
        return new KafkaTemplate<>(dltProducerFactory);
    }
}
