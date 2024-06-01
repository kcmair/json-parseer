package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.RequestModel;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

import java.util.*;

@Service
public class DynamoDBService {

    private final String tableName = "codes_by_date";
    private final DynamoDbClient client = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();
    private final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(client)
            .build();
    private final DynamoDbTable<CodeByDate> table = enhancedClient.table(tableName, TableSchema.fromBean(CodeByDate.class));
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void writeData(RequestModel request) {
        try {
            JsonNode rootNode = objectMapper.readTree(request.getJsonData());

            // Convert JSON data to a map with date as key and list of sampleIds as value
            Map<String, List<String>> dataMap = new TreeMap<>(Collections.reverseOrder());
            for (JsonNode itemNode : rootNode.get("Items")) {
                String date = itemNode.get("sampleFolder").get("S").asText();
                String sampleId = itemNode.get("sampleId").get("S").asText();
                dataMap.computeIfAbsent(date, _ -> new ArrayList<>()).add(sampleId);
            }

            // Write data to DynamoDB
            for (Map.Entry<String, List<String>> entry : dataMap.entrySet()) {
                String date = entry.getKey();
                List<String> sampleIds = entry.getValue();

                CodeByDate codeByDate = new CodeByDate();
                codeByDate.setDate(date);
                codeByDate.setSampleIds(sampleIds);

                PutItemEnhancedRequest<CodeByDate> putRequest = PutItemEnhancedRequest.builder(CodeByDate.class)
                        .item(codeByDate)
                        .build();
                table.putItem(putRequest);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> readData(String date) {
        CodeByDate codeByDate = table.getItem(r -> r.key(k -> k.partitionValue(date)));
        return codeByDate != null ? codeByDate.getSampleIds() : Collections.emptyList();
    }

    public void deleteData(String date) {
        table.deleteItem(r -> r.key(k -> k.partitionValue(date)));
    }

    @DynamoDbBean
    public static class CodeByDate {
        private String date;
        private List<String> sampleIds;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public List<String> getSampleIds() {
            return sampleIds;
        }

        public void setSampleIds(List<String> sampleIds) {
            this.sampleIds = sampleIds;
        }
    }
}
