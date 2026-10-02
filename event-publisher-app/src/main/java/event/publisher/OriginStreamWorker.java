package event.publisher;

import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsSchedulerFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsPollingConfig;
import event.common.dynamodb.config.DynamoDbProperties;
import event.common.events.EventSubmission;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.util.UUID;

import static event.common.dynamodb.DynamoDbTableNames.ORIGIN;

@Slf4j
@Component
public class OriginStreamWorker implements ApplicationRunner, DisposableBean {
    private final DynamoDbClient db;
    private final DynamoDbProperties properties;
    private final KafkaTemplate<String, EventSubmission> kafka;
    private final JsonMapper mapper;
    private Scheduler scheduler;
    private Thread thread;
    private DynamoDbAsyncClient leases;
    private DynamoDbStreamsClient streams;
    private KinesisAsyncClient kinesis;
    private CloudWatchAsyncClient cloudWatch;

    public OriginStreamWorker(DynamoDbClient db, DynamoDbProperties properties,
                              KafkaTemplate<String, EventSubmission> kafka, JsonMapper mapper) {
        this.db = db;
        this.properties = properties;
        this.kafka = kafka;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments arguments) {
        String arn = db.describeTable(builder -> builder.tableName(ORIGIN)).table().latestStreamArn();
        if (arn == null || arn.isBlank()) throw new IllegalStateException("ORIGIN DynamoDB Stream is required");
        Region region = Region.of(properties.getRegion());
        var credentials = properties.getEndpoint() == null || properties.getEndpoint().isBlank()
                ? DefaultCredentialsProvider.create()
                : StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"));
        var leasesBuilder = DynamoDbAsyncClient.builder().region(region).credentialsProvider(credentials);
        var streamsBuilder = DynamoDbStreamsClient.builder().region(region).credentialsProvider(credentials);
        if (properties.getEndpoint() != null && !properties.getEndpoint().isBlank()) {
            URI endpoint = URI.create(properties.getEndpoint());
            leasesBuilder.endpointOverride(endpoint);
            streamsBuilder.endpointOverride(endpoint);
        }
        leases = leasesBuilder.build();
        streams = streamsBuilder.build();
        kinesis = KinesisAsyncClient.builder().region(region).credentialsProvider(credentials).build();
        cloudWatch = CloudWatchAsyncClient.builder().region(region).credentialsProvider(credentials).build();
        var tracker = StreamsSchedulerFactory.createSingleStreamTracker(arn,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        var configs = new ConfigsBuilder(tracker, "event-publisher-app", kinesis, leases,
                cloudWatch, UUID.randomUUID().toString(), () -> new OriginStreamProcessor(db, kafka, mapper));
        configs.metricsConfig().metricsLevel(MetricsLevel.NONE);
        configs.retrievalConfig().retrievalSpecificConfig(new DynamoDBStreamsPollingConfig(kinesis));
        scheduler = StreamsSchedulerFactory.createScheduler(configs.checkpointConfig(),
                configs.coordinatorConfig(), configs.leaseManagementConfig(), configs.lifecycleConfig(),
                configs.metricsConfig(), configs.processorConfig(), configs.retrievalConfig(), streams, region);
        thread = new Thread(scheduler, "origin-stream-scheduler");
        thread.setDaemon(false);
        thread.start();
        log.info("ORIGIN Stream publisher started: {}", arn);
    }

    @Override
    public void destroy() throws Exception {
        if (scheduler != null) scheduler.startGracefulShutdown().get();
        if (thread != null) thread.join(10_000);
        if (streams != null) streams.close();
        if (leases != null) leases.close();
        if (kinesis != null) kinesis.close();
        if (cloudWatch != null) cloudWatch.close();
    }
}
