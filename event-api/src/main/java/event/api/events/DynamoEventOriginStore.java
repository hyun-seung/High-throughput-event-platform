package event.api.events;

import event.common.events.EventOriginCodec;
import event.common.events.EventSubmission;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

import static event.common.dynamodb.DynamoDbTableNames.ORIGIN;

@Repository
public class DynamoEventOriginStore implements EventOriginStore {
    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public DynamoEventOriginStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    @Override
    public void save(EventSubmission event) {
        db.putItem(PutItemRequest.builder().tableName(ORIGIN)
                .item(EventOriginCodec.encode(event, mapper))
                .conditionExpression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
                .build());
    }

    @Override
    public Optional<EventSubmission> find(String executionId) {
        var item = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(EventOriginCodec.key(executionId)).consistentRead(true).build()).item();
        return item.isEmpty() ? Optional.empty() : Optional.of(EventOriginCodec.decode(item, mapper));
    }
}
