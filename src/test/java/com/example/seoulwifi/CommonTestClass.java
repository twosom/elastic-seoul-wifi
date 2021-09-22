package com.example.seoulwifi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

@SpringBootTest
class CommonTestClass {

    @Value("${openapi.key}")
    String openApiKey;

    RestHighLevelClient client;

    RestTemplate restTemplate;

    @BeforeEach
    void beforeEach() {
        client = createRestHighLevelClient();
        restTemplate = new RestTemplate();
    }

    private RestHighLevelClient createRestHighLevelClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200)
        );

        return new RestHighLevelClient(restClientBuilder);
    }

    protected void removeIfExistsIndex(String index) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            Assertions.assertTrue(deleteIndexResponse.isAcknowledged());
        }
    }



}
