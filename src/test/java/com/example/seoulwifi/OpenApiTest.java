package com.example.seoulwifi;


import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenApiTest extends CommonTestClass {


    @DisplayName("오픈 API 데이터를 가져온다.")
    @Test
    void get_open_api_data() throws Exception {
        String url = String.format("http://openapi.seoul.go.kr:8088/%s/json/TbPublicWifiInfo/1/500", openApiKey);
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
        List<Map<String, Object>> row = (List<Map<String, Object>>) ((Map<String, Object>) response.getBody()
                .get("TbPublicWifiInfo"))
                .get("row");

        row.forEach(System.err::println);
    }


    @DisplayName("seoul_wifi 인덱스 생성")
    @Test
    void create_index_for_seoul_wifi() throws Exception {
        removeIfExistsIndex("seoul_wifi");

        XContentBuilder settingBuilder = XContentFactory.jsonBuilder();
        settingBuilder.startObject();
        {
            settingBuilder.startObject("analysis");
            {
                settingBuilder.startObject("analyzer");
                {
                    settingBuilder.startObject("korean");
                    {
                        settingBuilder.field("tokenizer", "nori_tokenizer");
                    }
                    settingBuilder.endObject();
                }
                settingBuilder.endObject();
            }
            settingBuilder.endObject();
        }
        settingBuilder.endObject();


        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();


        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("gu_nm");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();

                mappingBuilder.startObject("place_nm");
                {
                    mappingBuilder.field("type", "text");
                    mappingBuilder.field("analyzer", "korean");
                }
                mappingBuilder.endObject();


                mappingBuilder.startObject("instl_xy");
                {
                    mappingBuilder.field("type", "geo_point");
                }
                mappingBuilder.endObject();


            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("seoul_wifi")
                .settings(settingBuilder)
                .mapping(mappingBuilder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("최종 코드")
    @Test
    void final_code() throws Exception {
        System.err.print("Start Indexing");
        for (int i = 1; i < 14; i++) {
            int iStart = (i - 1) * 1000 + 1;
            int iEnd = i * 1000;


            String url = String.format("http://openapi.seoul.go.kr:8088/%s/json/TbPublicWifiInfo/%d/%d", openApiKey, iStart, iEnd);
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            List<Map<String, Object>> row = (List<Map<String, Object>>) ((Map<String, Object>) response.getBody()
                    .get("TbPublicWifiInfo"))
                    .get("row");

            List<Map<String, Object>> collect = row.stream()
                    .map(e -> {
                        String gu_nm = e.get("X_SWIFI_WRDOFC").toString();
                        String place_nm = e.get("X_SWIFI_MAIN_NM").toString();
                        String place_x = e.get("LAT").toString();
                        String place_y = e.get("LNT").toString();
                        return Map.of(
                                "gu_nm", gu_nm,
                                "place_nm", place_nm,
                                "instl_xy", Map.of(
                                        "lat", place_y,
                                        "lon", place_x
                                )
                        );
                    })
                    .distinct()
                    .collect(Collectors.toList());

            BulkRequest bulkRequest = new BulkRequest();
            collect.forEach(e -> bulkRequest.add(
                    new IndexRequest()
                            .index("seoul_wifi")
                            .source(e)
            ));
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            boolean result = Arrays.stream(bulkResponse.getItems())
                    .map(BulkItemResponse::getOpType)
                    .allMatch(e -> e.equals(DocWriteRequest.OpType.INDEX));
            assertTrue(result);
        }
        System.err.print("End Indexing");


    }

}
