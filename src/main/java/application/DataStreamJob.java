/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package application;

import com.fasterxml.jackson.core.JsonProcessingException;
import deserialization.JsonValueDeserializationSchema;
import dto.Delivery;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sinks.DatabaseSink;
import utils.PropertyUtil;

import java.util.Properties;


import static utils.JsonUtil.convertDeliveryDataToJson;

public class DataStreamJob {


    public static void main(String[] args) throws Exception {
        Properties prop = PropertyUtil.getProperties();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        KafkaSource<Delivery> source = KafkaSource
                .<Delivery>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("TOPIC"))
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonValueDeserializationSchema())
                .build();

        DataStream<Delivery> deliveryStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        DatabaseSink databaseSink = new DatabaseSink(prop);
        databaseSink.setupDatabaseSinks(deliveryStream);

        Elasticsearch7SinkBuilder<Delivery> elasticsearchSinkBuilder = new Elasticsearch7SinkBuilder<>()
                .setHosts(new HttpHost("localhost", 9200, "http"))
                .setEmitter((delivery, context, indexer) -> {
                    try {
                        String json = convertDeliveryDataToJson(delivery);

                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("delivery")
                                .id(delivery.getDeliveryId())
                                .create(true)
                                .source(json, XContentType.JSON);
                        indexer.add(indexRequest);
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Failed converting Delivery Data to JSON: {}", e.getMessage());
                    }
                });

        // create elasticsearch sink
        deliveryStream.sinkTo(
                elasticsearchSinkBuilder.build()
        ).name("Elasticsearch sink");

        // Execute program, beginning computation.
        env.execute("Delivery Realtime Data Streaming");
    }
}
