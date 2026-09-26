package event.common.recovery;

import org.junit.jupiter.api.Test;
import java.sql.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import static org.junit.jupiter.api.Assertions.*;

class StorageFailureTest {
    @Test void recognizesWrappedSqlConnectionsTimeoutsAndOverload() {
        for (String state : java.util.List.of("08006", "53300", "57P01", "57014", "55P03"))
            assertTrue(StorageFailure.unavailable(new IllegalStateException("wrapped", new SQLException("SQL error", state))));
        assertTrue(StorageFailure.unavailable(new SQLTransientConnectionException("pool wait")));
    }
    @Test void dataErrorsAndConditionalConflictsDoNotPauseOtherRecords() {
        assertFalse(StorageFailure.unavailable(new SQLException("duplicate", "23505")));
        assertFalse(StorageFailure.unavailable(new IllegalStateException("different immutable result")));
        assertFalse(StorageFailure.unavailable(ConditionalCheckFailedException.builder().statusCode(400).build()));
        assertFalse(StorageFailure.unavailable(TransactionCanceledException.builder()
                .cancellationReasons(CancellationReason.builder().code("ConditionalCheckFailed").build()).build()));
    }
    @Test void recognizesDirectAndTransactionalDynamoThrottling() {
        assertTrue(StorageFailure.unavailable(DynamoDbException.builder().statusCode(503).build()));
        assertTrue(StorageFailure.unavailable(ProvisionedThroughputExceededException.builder().statusCode(400).build()));
        assertTrue(StorageFailure.unavailable(TransactionCanceledException.builder()
                .cancellationReasons(CancellationReason.builder().code("ThrottlingError").build()).build()));
    }
}
