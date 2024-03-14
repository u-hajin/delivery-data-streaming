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

import deserialization.JsonValueDeserializationSchema;
import dto.Delivery;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String userName = "postgres";
    private static final String password = "postgres";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "delivery_information";

        KafkaSource<Delivery> source = KafkaSource
                .<Delivery>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonValueDeserializationSchema())
                .build();

        DataStream<Delivery> deliveryStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
//        deliveryStream.print();

        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(userName)
                .withPassword(password)
                .build();


        // create delivery_information table
        deliveryStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS delivery_information(" +
                        "delivery_id VARCHAR(255) PRIMARY KEY, " +
                        "delivery_date TIMESTAMP, " +
                        "user_id VARCHAR(255), " +
                        "food_category VARCHAR(100), " +
                        "food_price DOUBLE PRECISION, " +
                        "payment_method VARCHAR(100), " +
                        "delivery_distance DOUBLE PRECISION, " +
                        "delivery_destination VARCHAR(255), " +
                        "destination_lat DECIMAL(17, 14), " +
                        "destination_lon DECIMAL(17, 14), " +
                        "delivery_charge INT " +
                        ")",
                (JdbcStatementBuilder<Delivery>) (preparedStatement, delivery) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create delivery_information table");

        // insert into delivery_information table
        deliveryStream.addSink(JdbcSink.sink(
                "INSERT INTO delivery_information(delivery_id, delivery_date, user_id, food_category, food_price, " +
                        "payment_method, delivery_distance, delivery_destination, destination_lat, destination_lon, delivery_charge) " +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (delivery_id) DO UPDATE SET " +
                        "delivery_date = EXCLUDED.delivery_date, " +
                        "user_id = EXCLUDED.user_id, " +
                        "food_category = EXCLUDED.food_category, " +
                        "food_price = EXCLUDED.food_price, " +
                        "payment_method = EXCLUDED.payment_method, " +
                        "delivery_distance = EXCLUDED.delivery_distance, " +
                        "delivery_destination = EXCLUDED.delivery_destination, " +
                        "destination_lat = EXCLUDED.destination_lat, " +
                        "destination_lon = EXCLUDED.destination_lon, " +
                        "delivery_charge = EXCLUDED.delivery_charge " +
                        "WHERE delivery_information.delivery_id = EXCLUDED.delivery_id",
                (JdbcStatementBuilder<Delivery>) (preparedStatement, delivery) -> {
                    preparedStatement.setString(1, delivery.getDeliveryId());
                    preparedStatement.setTimestamp(2, delivery.getDeliveryDate());
                    preparedStatement.setString(3, delivery.getUserId());
                    preparedStatement.setString(4, delivery.getFoodCategory());
                    preparedStatement.setDouble(5, delivery.getFoodPrice());
                    preparedStatement.setString(6, delivery.getPaymentMethod());
                    preparedStatement.setDouble(7, delivery.getDeliveryDistance());
                    preparedStatement.setString(8, delivery.getDeliveryDestination());
                    preparedStatement.setDouble(9, delivery.getDestinationLat());
                    preparedStatement.setDouble(10, delivery.getDestinationLon());
                    preparedStatement.setInt(11, delivery.getDeliveryCharge());
                },
                executionOptions,
                connectionOptions
        )).name("Insert into delivery_information table");


        // Execute program, beginning computation.
        env.execute("Delivery Realtime Data Streaming");
    }
}
