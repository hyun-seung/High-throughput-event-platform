package event.common.recovery;

import java.sql.*;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Availability/overload failures only; data conflicts and failed conditional writes remain per-record outcomes. */
public final class StorageFailure {
    private StorageFailure() { }
    public static boolean unavailable(Throwable failure) {
        for (int depth = 0; failure != null && depth < 12; depth++, failure = failure.getCause()) {
            if (failure instanceof DataAccessResourceFailureException || failure instanceof QueryTimeoutException
                    || failure instanceof SQLTransientException || failure instanceof SQLRecoverableException
                    || failure instanceof SQLNonTransientConnectionException || failure instanceof SdkClientException
                    || failure instanceof java.util.concurrent.TimeoutException) return true;
            if (failure instanceof SQLException sql && sql.getSQLState() != null
                    && (sql.getSQLState().startsWith("08") || sql.getSQLState().startsWith("53")
                    || java.util.Set.of("57014", "57P01", "57P02", "57P03", "55P03").contains(sql.getSQLState()))) return true;
            if (failure instanceof DynamoDbException db && (db.statusCode() >= 500 || db.statusCode() == 429
                    || db instanceof ProvisionedThroughputExceededException || db instanceof RequestLimitExceededException)) return true;
            if (failure instanceof TransactionCanceledException tx && tx.cancellationReasons().stream()
                    .anyMatch(r -> "ProvisionedThroughputExceeded".equals(r.code()) || "ThrottlingError".equals(r.code()))) return true;
        }
        return false;
    }
}
