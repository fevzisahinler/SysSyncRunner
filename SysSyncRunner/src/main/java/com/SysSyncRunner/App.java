package com.SysSyncRunner;

import io.github.cdimascio.dotenv.Dotenv;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class App {
    public static void main(String[] args) throws InterruptedException, IOException {
        Dotenv dotenv = Dotenv.load();
        String INFLUXDB_URL = dotenv.get("INFLUXDB_URL");
        String INFLUXDB_TOKEN = dotenv.get("INFLUXDB_TOKEN");
        String INFLUXDB_ORG = dotenv.get("INFLUXDB_ORG");
        String INFLUXDB_BUCKET_NAME = dotenv.get("INFLUXDB_BUCKET_NAME");
        String ELASTICSEARCH_HOST = dotenv.get("ELASTICSEARCH_HOST");
        int ELASTICSEARCH_PORT = Integer.parseInt(dotenv.get("ELASTICSEARCH_PORT"));
        String ELASTICSEARCH_SCHEME = dotenv.get("ELASTICSEARCH_SCHEME");
        String ELASTICSEARCH_INDEX = dotenv.get("ELASTICSEARCH_INDEX");

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(INFLUXDB_URL, INFLUXDB_TOKEN.toCharArray(), INFLUXDB_ORG, INFLUXDB_BUCKET_NAME);
        QueryApi queryApi = influxDBClient.getQueryApi();
        RestHighLevelClient elasticsearchClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_SCHEME)));

        while (true) {
            List<FluxTable> hostTables = queryApi.query(
                    "from(bucket:\"" + INFLUXDB_BUCKET_NAME + "\")" +
                            " |> range(start: -1h)" +
                            " |> keep(columns: [\"_value\", \"_time\", \"host\"])" +
                            " |> distinct(column: \"host\")");

            for (FluxTable hostTable : hostTables) {
                for (FluxRecord hostRecord : hostTable.getRecords()) {
                    Object host = hostRecord.getValueByKey("host");
                    if (host != null) {
                        List<FluxTable> metricTables = queryApi.query(
                                String.format(
                                        "from(bucket:\"" + INFLUXDB_BUCKET_NAME + "\")" +
                                                " |> range(start: -5s)" +
                                                " |> filter(fn: (r) => r[\"_measurement\"] == \"system_metrics\")" +
                                                " |> filter(fn: (r) => r[\"_field\"] == \"total_cpu\" or r[\"_field\"] == \"total_memory\" or r[\"_field\"] == \"used_cpu\" or r[\"_field\"] == \"used_memory\")" +
                                                " |> filter(fn: (r) => r[\"host\"] == \"%s\")" +
                                                " |> yield(name: \"mean\")",  host));

                        for (FluxTable metricTable : metricTables) {
                            for (FluxRecord metricRecord : metricTable.getRecords()) {
                                Map<String, Object> jsonMap = new HashMap<>();
                                jsonMap.put("time", metricRecord.getTime());
                                metricRecord.getValues().forEach(jsonMap::put);

                                String uniqueId = UUID.nameUUIDFromBytes((metricRecord.getTime().toString() + host.toString() + metricRecord.getMeasurement()).getBytes()).toString();

                                IndexRequest request = new IndexRequest(ELASTICSEARCH_INDEX)
                                        .id(uniqueId)
                                        .source(jsonMap)
                                        .opType("create");
                                try {
                                    IndexResponse response = elasticsearchClient.index(request, RequestOptions.DEFAULT);
                                    System.out.println("Indexed document ID: " + response.getId() + " with status: " + response.status().name());
                                } catch (Exception e) {
                                    System.out.println("Error indexing document with ID: " + uniqueId + " - " + e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
            Thread.sleep(5);
        }
    }
}
