package messaging.api.messages;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

@Repository
public class DynamoMessageOriginStore implements MessageOriginStore {
    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public DynamoMessageOriginStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    @Override
    public void save(MessageSubmission event) {
        db.putItem(PutItemRequest.builder().tableName(ORIGIN)
                .item(MessageOriginCodec.encode(event, mapper))
                .conditionExpression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
                .build());
    }

    @Override
    public Optional<MessageSubmission> find(String clientMsgId) {
        var item = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(clientMsgId)).consistentRead(true).build()).item();
        return item.isEmpty() ? Optional.empty() : Optional.of(MessageOriginCodec.decode(item, mapper));
    }
}
