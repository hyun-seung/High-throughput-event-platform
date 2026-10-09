package messaging.complete;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.PrimaryStageDecision;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Stores one immutable final message and, only for primary success, one billable CDR. */
@Repository
public class FinalizedHistoryStore {
    static final int MAX_BATCH_SIZE = 100;
    private static final String INSERT_MESSAGE = """
            INSERT INTO tbl_msg_hist (client_msg_id, decision_id, client_id, message_id,
                recipient_number, message_category, final_stage, outcome, result_source,
                carrier, invocation, error_code, reason, received_at, decided_at,
                result_json, submission_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
            ON CONFLICT (client_msg_id) DO NOTHING
            """;
    private static final String INSERT_CDR = """
            INSERT INTO tbl_cdr_hist (client_msg_id, decision_id, client_id, message_id,
                carrier, billable_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (client_msg_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final JsonMapper mapper;
    private final DistributionSummary batchSize;
    private final Timer batchTime;

    public FinalizedHistoryStore(JdbcTemplate jdbc, PlatformTransactionManager manager, JsonMapper mapper,
                                 MeterRegistry metrics) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.transaction = new TransactionTemplate(Objects.requireNonNull(manager));
        this.transaction.setTimeout(10);
        this.mapper = Objects.requireNonNull(mapper);
        this.batchSize = DistributionSummary.builder("messaging.complete.sql.batch.size")
                .description("Records in successfully committed completion batches, including replays")
                .register(metrics);
        this.batchTime = Timer.builder("messaging.complete.sql.batch.duration")
                .description("Completion database transaction duration, including failed attempts")
                .register(metrics);
    }

    public void store(FinalizedMessageResult finalized) {
        storeBatch(List.of(finalized));
    }

    /** The whole poll commits or rolls back; replay never changes an existing final result. */
    public void storeBatch(List<FinalizedMessageResult> finalized) {
        Objects.requireNonNull(finalized);
        if (finalized.size() > MAX_BATCH_SIZE) throw new IllegalArgumentException("Completion batch exceeds 100 messages");
        if (finalized.isEmpty()) return;
        // Stable lock order also covers overlapping batches delivered by different consumers.
        var rows = finalized.stream().map(this::prepare)
                .sorted(Comparator.comparing(Prepared::clientMsgId)).toList();
        var billable = rows.stream().filter(row -> row.cdrArguments() != null).toList();
        batchTime.record(() -> transaction.executeWithoutResult(ignored -> {
            jdbc.batchUpdate(INSERT_MESSAGE, rows.stream().map(Prepared::messageArguments).toList());
            var messageChecks = new ArrayList<Object>();
            for (var row : rows) Collections.addAll(messageChecks, row.messageCheck());
            Integer matching = jdbc.queryForObject("""
                    SELECT count(*) FROM tbl_msg_hist h JOIN (VALUES
                    """ + tuples(rows.size(), "(?, ?, ?::jsonb, ?::jsonb)") + """
                    ) AS expected(client_msg_id, decision_id, result_json, submission_json)
                    ON h.client_msg_id = expected.client_msg_id
                    WHERE h.decision_id = expected.decision_id AND h.result_json = expected.result_json
                        AND h.submission_json = expected.submission_json
                    """, Integer.class, messageChecks.toArray());
            if (!Integer.valueOf(rows.size()).equals(matching)) {
                throw new IllegalStateException("Conflicting final result in completion batch");
            }
            if (billable.isEmpty()) return;
            jdbc.batchUpdate(INSERT_CDR, billable.stream().map(Prepared::cdrArguments).toList());
            var cdrChecks = new ArrayList<Object>();
            for (var row : billable) Collections.addAll(cdrChecks, row.cdrArguments());
            Integer matchingCdr = jdbc.queryForObject("""
                    SELECT count(*) FROM tbl_cdr_hist c JOIN (VALUES
                    """ + tuples(billable.size(), "(?, ?, ?::bigint, ?, ?, ?::timestamptz)") + """
                    ) AS expected(client_msg_id, decision_id, client_id, message_id, carrier, billable_at)
                    ON c.client_msg_id = expected.client_msg_id
                    WHERE c.decision_id = expected.decision_id AND c.client_id = expected.client_id
                        AND c.message_id = expected.message_id AND c.carrier = expected.carrier
                        AND c.billable_at = expected.billable_at AND c.billing_status = 'BILLABLE'
                    """, Integer.class, cdrChecks.toArray());
            if (!Integer.valueOf(billable.size()).equals(matchingCdr)) {
                throw new IllegalStateException("Conflicting CDR in completion batch");
            }
        }));
        batchSize.record(rows.size());
    }

    private Prepared prepare(FinalizedMessageResult finalized) {
        Objects.requireNonNull(finalized);
        var decision = finalized.decision();
        var secondary = finalized.secondaryDecision();
        var submission = finalized.submission();
        if (!decision.clientMsgId().equals(submission.clientMsgId())
                || secondary == null && (decision.secondaryRequired()
                || decision.kind() == PrimaryStageDecision.Kind.SUCCESS && decision.carrier() == null)
                || secondary != null && (!decision.secondaryRequired()
                || !secondary.clientMsgId().equals(submission.clientMsgId()))) {
            throw new IllegalArgumentException("Invalid finalized result");
        }
        String resultJson = mapper.writeValueAsString(secondary == null ? decision : secondary);
        String submissionJson = mapper.writeValueAsString(submission);
        String decisionId = secondary == null ? decision.decisionId() : secondary.decisionId();
        String outcome = secondary == null ? decision.kind().name() : secondary.kind().name();
        String stage = secondary == null ? "PRIMARY" : "SECONDARY";
        Object[] message = {submission.clientMsgId(), decisionId,
                    submission.clientId(), submission.messageId(), submission.recipientNumber(),
                    submission.messageCategory().name(), stage, outcome,
                    secondary == null ? decision.source() : "TCP_RESPONSE",
                    decision.carrier() == null ? null : decision.carrier().name(),
                    secondary == null ? decision.invocation() : null,
                    secondary == null ? decision.errorCode() : secondary.errorCode(),
                    secondary == null ? decision.reason() : secondary.providerCode(),
                    Timestamp.from(submission.receivedAt()),
                    Timestamp.from(secondary == null ? decision.decidedAt() : secondary.decidedAt()), resultJson, submissionJson};
        Object[] cdr = secondary == null && decision.kind() == PrimaryStageDecision.Kind.SUCCESS
                ? new Object[] {submission.clientMsgId(), decision.decisionId(), submission.clientId(),
                    submission.messageId(), decision.carrier().name(), Timestamp.from(decision.decidedAt())} : null;
        return new Prepared(submission.clientMsgId(), message,
                new Object[] {submission.clientMsgId(), decisionId, resultJson, submissionJson}, cdr);
    }

    private static String tuples(int count, String tuple) {
        return String.join(",", Collections.nCopies(count, tuple));
    }

    private record Prepared(String clientMsgId, Object[] messageArguments, Object[] messageCheck,
                            Object[] cdrArguments) { }
}
