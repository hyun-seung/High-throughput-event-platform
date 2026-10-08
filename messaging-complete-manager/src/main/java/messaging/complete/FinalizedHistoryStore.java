package messaging.complete;

import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.PrimaryStageDecision;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Timestamp;
import java.util.Objects;

/** Stores one immutable final message and, only for primary success, one billable CDR. */
@Repository
public class FinalizedHistoryStore {
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

    public FinalizedHistoryStore(JdbcTemplate jdbc, PlatformTransactionManager manager, JsonMapper mapper) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.transaction = new TransactionTemplate(Objects.requireNonNull(manager));
        this.transaction.setTimeout(10);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public void store(FinalizedMessageResult finalized) {
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
        transaction.executeWithoutResult(ignored -> {
            jdbc.update(INSERT_MESSAGE, submission.clientMsgId(), decisionId,
                    submission.clientId(), submission.messageId(), submission.recipientNumber(),
                    submission.messageCategory().name(), stage, outcome,
                    secondary == null ? decision.source() : "TCP_RESPONSE",
                    decision.carrier() == null ? null : decision.carrier().name(),
                    secondary == null ? decision.invocation() : null,
                    secondary == null ? decision.errorCode() : secondary.errorCode(),
                    secondary == null ? decision.reason() : secondary.providerCode(),
                    Timestamp.from(submission.receivedAt()),
                    Timestamp.from(secondary == null ? decision.decidedAt() : secondary.decidedAt()), resultJson, submissionJson);
            Boolean sameMessage = jdbc.queryForObject("""
                    SELECT decision_id = ? AND result_json = ?::jsonb AND submission_json = ?::jsonb
                    FROM tbl_msg_hist WHERE client_msg_id = ?
                    """, Boolean.class, decisionId, resultJson, submissionJson,
                    submission.clientMsgId());
            if (!Boolean.TRUE.equals(sameMessage)) {
                throw new IllegalStateException("Conflicting final result for clientMsgId=" + submission.clientMsgId());
            }
            if (secondary != null || decision.kind() != PrimaryStageDecision.Kind.SUCCESS) return;
            jdbc.update(INSERT_CDR, submission.clientMsgId(), decision.decisionId(),
                    submission.clientId(), submission.messageId(), decision.carrier().name(),
                    Timestamp.from(decision.decidedAt()));
            Boolean sameCdr = jdbc.queryForObject("""
                    SELECT decision_id = ? AND client_id = ? AND message_id = ? AND carrier = ?
                        AND billable_at = ? AND billing_status = 'BILLABLE'
                    FROM tbl_cdr_hist WHERE client_msg_id = ?
                    """, Boolean.class, decision.decisionId(), submission.clientId(), submission.messageId(),
                    decision.carrier().name(), Timestamp.from(decision.decidedAt()), submission.clientMsgId());
            if (!Boolean.TRUE.equals(sameCdr)) {
                throw new IllegalStateException("Conflicting CDR for clientMsgId=" + submission.clientMsgId());
            }
        });
    }
}
