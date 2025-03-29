package com.task06;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
    lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {

	private static final String AUDIT_TABLE = System.getenv("target_table");

	@Override
	public Void handleRequest(DynamodbEvent event, Context context) {
		DynamoDbClient dynamoDbClient = DynamoDbClient.create();

		try {
			for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
				processRecord(record, dynamoDbClient);
			}
			return null;
		} finally {
			dynamoDbClient.close();
		}
	}

	private void processRecord(DynamodbEvent.DynamodbStreamRecord record, DynamoDbClient dynamoDbClient) {
		String eventName = record.getEventName();

		if ("INSERT".equals(eventName) || "MODIFY".equals(eventName)) {
			Map<String, AttributeValue> newImage = convertToAttributeValueMap(record.getDynamodb().getNewImage());
			Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage() != null ?
					convertToAttributeValueMap(record.getDynamodb().getOldImage()) : null;

			String itemKey = newImage.get("key").s();
			String modificationTime = Instant.now().toString();

			Map<String, AttributeValue> auditItem = new HashMap<>();
			auditItem.put("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
			auditItem.put("itemKey", AttributeValue.builder().s(itemKey).build());
			auditItem.put("modificationTime", AttributeValue.builder().s(modificationTime).build());

			if ("INSERT".equals(eventName)) {

				Map<String, AttributeValue> newValueMap = new HashMap<>();
				newValueMap.put("key", AttributeValue.builder().s(itemKey).build());
				newValueMap.put("value", AttributeValue.builder().n(newImage.get("value").n()).build());

				auditItem.put("newValue", AttributeValue.builder().m(newValueMap).build());
			} else if ("MODIFY".equals(eventName)) {

				auditItem.put("oldValue", AttributeValue.builder().n(oldImage.get("value").n()).build());
				auditItem.put("newValue", AttributeValue.builder().n(newImage.get("value").n()).build());
				auditItem.put("updatedAttribute", AttributeValue.builder().s("value").build());
			}

			PutItemRequest putItemRequest = PutItemRequest.builder()
					.tableName(AUDIT_TABLE)
					.item(auditItem)
					.build();

			PutItemResponse response = dynamoDbClient.putItem(putItemRequest);
		}
	}

	private Map<String, AttributeValue> convertToAttributeValueMap(Map<String, com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> source) {
		Map<String, AttributeValue> result = new HashMap<>();
		source.forEach((key, value) -> {
			if (value.getS() != null) {
				result.put(key, AttributeValue.builder().s(value.getS()).build());
			} else if (value.getN() != null) {
				result.put(key, AttributeValue.builder().n(value.getN()).build());
			}
		});
		return result;
	}
}