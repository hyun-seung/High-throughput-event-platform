package event.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/** Duration counts logical calls; the separate transport counter includes SDK attempts, not consumed capacity. */
public final class DynamoDbMetricsInterceptor implements ExecutionInterceptor {
    private static final ExecutionAttribute<Timer.Sample> SAMPLE = new ExecutionAttribute<>("delivery.db.sample");
    private final MeterRegistry registry;

    public DynamoDbMetricsInterceptor(MeterRegistry registry) { this.registry = registry; }

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes attributes) {
        attributes.putAttribute(SAMPLE, Timer.start(registry));
    }

    @Override
    public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
        registry.counter("delivery.dynamodb.transport.attempts", "operation", operation(context.request().getClass().getSimpleName())).increment();
    }

    @Override
    public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
        finish(context.request().getClass().getSimpleName(), "success", attributes);
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes attributes) {
        finish(context.request().getClass().getSimpleName(),
                context.exception() instanceof ConditionalCheckFailedException ? "condition_failed" : "failure", attributes);
    }

    private void finish(String request, String result, ExecutionAttributes attributes) {
        Timer.Sample sample = attributes.getAttribute(SAMPLE);
        if (sample != null) sample.stop(Timer.builder("delivery.dynamodb.duration")
                .tags("operation", operation(request), "result", result).register(registry));
    }

    private static String operation(String request) {
        return switch (request) {
            case "QueryRequest" -> "query";
            case "GetItemRequest" -> "get_item";
            case "PutItemRequest" -> "put_item";
            case "UpdateItemRequest" -> "update_item";
            case "TransactWriteItemsRequest" -> "transact_write_items";
            case "DescribeTableRequest" -> "describe_table";
            case "CreateTableRequest" -> "create_table";
            default -> "other";
        };
    }
}
