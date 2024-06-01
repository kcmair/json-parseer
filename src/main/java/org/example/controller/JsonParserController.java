package org.example.controller;

import org.example.service.DynamoDBService;
import org.example.model.RequestModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class JsonParserController {

    @Autowired
    private DynamoDBService dynamoDBService;

    @PostMapping("/write")
    public ResponseEntity<String> writeData(@RequestBody RequestModel request) {
        dynamoDBService.writeData(request);
        return ResponseEntity.ok("Data written successfully");
    }

    @GetMapping("/read")
    public ResponseEntity<?> readData(@RequestParam String date) {
        return ResponseEntity.ok(dynamoDBService.readData(date));
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> deleteData(@RequestParam String date) {
        dynamoDBService.deleteData(date);
        return ResponseEntity.ok("Data deleted successfully");
    }
}
