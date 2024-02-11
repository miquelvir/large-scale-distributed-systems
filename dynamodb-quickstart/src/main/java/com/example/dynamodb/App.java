/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

package com.example.dynamodb;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;

public class App {
    public static void main(String[] args) {
        System.out.println("Creating client:\n");
        Region region = Region.US_EAST_1;

        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();

        System.out.println(">>>>>>>>>>>>>>>>>>> LIST ALL TABLES EXAMPLE");
        listTables(ddb);
        
        System.out.println(">>>>>>>>>>>>>>>>>>> PUT ITEM EXAMPLE");
        putFirstSong(ddb);

        System.out.println(">>>>>>>>>>>>>>>>>>> PUT (ANOTHER) ITEM EXAMPLE");
        putAnotherSong(ddb);

        System.out.println(">>>>>>>>>>>>>>>>>>> SCAN ALL ITEMS EXAMPLE");
        scanAllItems(ddb);

        System.out.println(">>>>>>>>>>>>>>>>>>> SCAN FILTERED ITEMS EXAMPLE");
        scanFilteredItems(ddb);
        

        ddb.close();
    }

    public static void putFirstSong(DynamoDbClient ddb) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("Artist", AttributeValue.builder().s("Leonard Cohen").build());
        itemValues.put("SongTitle", AttributeValue.builder().s("Take This Waltz").build());
        itemValues.put("Album", AttributeValue.builder().s("Poets").build());
        itemValues.put("Year", AttributeValue.builder().n("1986").build());
        itemValues.put("Awards", AttributeValue.builder().l(
            AttributeValue.builder().s("Super Award 1").build(),
            AttributeValue.builder().s("Super Award 2").build()
        ).build());
        itemValues.put("Genre", AttributeValue.builder().s("Folk").build());

        PutItemRequest request = PutItemRequest.builder()
            .tableName("Music")
            .item(itemValues)
            .build();

        PutItemResponse putresponse = ddb.putItem(request);
        System.out.println("Put item successfully:\n" + itemValues.toString() + "\n\n");
    }

    public static void scanAllItems(DynamoDbClient ddb) {
        ScanRequest scanRequest = ScanRequest.builder()
        .tableName("Music")
        .build();

        ScanResponse scanresponse = ddb.scan(scanRequest);
        for (Map<String, AttributeValue> item : scanresponse.items()) {
            System.out.println("Found an item:\n" + item.toString() + "\n\n");
        }
    }

    public static void scanFilteredItems(DynamoDbClient ddb) {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":genre", AttributeValue.builder().s("Folk").build());
        expressionAttributeValues.put(":year", AttributeValue.builder().n("1980").build());
        
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#yr", "Year");
        expressionAttributeNames.put("#gr", "Genre");


        ScanRequest scanRequest = ScanRequest.builder()
        .tableName("Music")
        .filterExpression("#gr = :genre and #yr > :year")
        .expressionAttributeNames(expressionAttributeNames)
        .expressionAttributeValues(expressionAttributeValues)
        .build();

        ScanResponse scanresponse = ddb.scan(scanRequest);
        for (Map<String, AttributeValue> item : scanresponse.items()) {
            System.out.println("Found an item:\n" + item.toString() + "\n\n");
        }
    }

    public static void putAnotherSong(DynamoDbClient ddb) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("Artist", AttributeValue.builder().s("Leonard Cohen").build());
        itemValues.put("SongTitle", AttributeValue.builder().s("Suzanne").build());
        itemValues.put("Album", AttributeValue.builder().s("Poets").build());
        itemValues.put("Year", AttributeValue.builder().n("1968").build());
        itemValues.put("Awards", AttributeValue.builder().l(
            AttributeValue.builder().s("Super Award 1").build(),
            AttributeValue.builder().s("Super Award 3").build()
        ).build());
        itemValues.put("Genre", AttributeValue.builder().s("Folk").build());

        PutItemRequest request = PutItemRequest.builder()
            .tableName("Music")
            .item(itemValues)
            .build();

        PutItemResponse putresponse = ddb.putItem(request);
        System.out.println("Put item successfully:\n" + itemValues.toString() + "\n\n");
    }

    public static void listTables(DynamoDbClient ddb) {
        boolean moreTables = true;
        String lastName = null;

        while (moreTables) {
            try {
                ListTablesResponse response = null;
                if (lastName == null) {
                    ListTablesRequest request = ListTablesRequest.builder().build();
                    response = ddb.listTables(request);
                } else {
                    ListTablesRequest request = ListTablesRequest.builder()
                            .exclusiveStartTableName(lastName).build();
                    response = ddb.listTables(request);
                }

                List<String> tableNames = response.tableNames();
                if (tableNames.size() > 0) {
                    System.out.println("Found tables in DynamoDB:");
                    for (String curName : tableNames) {
                        System.out.format("* %s\n", curName);
                    }
                    System.out.println("\n");
                } else {
                    System.out.println("No tables found!");
                    System.exit(0);
                }

                lastName = response.lastEvaluatedTableName();
                if (lastName == null) {
                    moreTables = false;
                }

            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }
}
