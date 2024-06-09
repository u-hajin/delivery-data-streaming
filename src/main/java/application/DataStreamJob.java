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
import dto.ChargePerDay;
import dto.Delivery;
import dto.PayPerCategory;
import dto.PayPerDestination;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Properties;
import java.util.StringTokenizer;

import static utils.JsonUtil.convertDeliveryDataToJson;

public class DataStreamJob {
    private static Properties getProperties() {
        Properties prop = new Properties();

        try (InputStream propsInput = DataStreamJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(propsInput);
            return prop;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        Properties prop = getProperties();
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

        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(prop.getProperty("JDBC_URL"))
                .withDriverName("org.postgresql.Driver")
                .withUsername(prop.getProperty("USER_NAME"))
                .withPassword(prop.getProperty("PASSWORD"))
                .build();

        String[] createTableStatements = {
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
                        "delivery_charge INTEGER " +
                        ")",
                "CREATE TABLE IF NOT EXISTS pay_per_destination(" +
                        "delivery_date DATE, " +
                        "delivery_destination VARCHAR(255), " +
                        "total_food_price DECIMAL, " +
                        "total_delivery_charge DECIMAL, " +
                        "PRIMARY KEY (delivery_date, delivery_destination)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS charge_per_day(" +
                        "month INTEGER, " +
                        "day VARCHAR(10), " +
                        "total_delivery_charge DECIMAL, " +
                        "PRIMARY KEY (month, day)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS pay_per_category(" +
                        "delivery_date DATE, " +
                        "food_category VARCHAR(100), " +
                        "total_food_price DECIMAL, " +
                        "PRIMARY KEY (delivery_date, food_category)" +
                        ")"
        };

        String[] sinkName = {
                "Create delivery_information table",
                "Create pay_per_destination table",
                "Create charge_per_day table",
                "Create pay_per_category table"
        };

        for (int i = 0; i < createTableStatements.length; i++) {
            deliveryStream.addSink(JdbcSink.sink(
                    createTableStatements[i],
                    (JdbcStatementBuilder<Delivery>) (preparedStatement, delivery) -> {
                    },
                    executionOptions,
                    connectionOptions
            )).name(sinkName[i]);
        }


        // insert into delivery_information table
        deliveryStream.addSink(JdbcSink.sink(
                "INSERT INTO delivery_information (delivery_id, delivery_date, user_id, food_category, food_price, " +
                        "payment_method, delivery_distance, delivery_destination, destination_lat, destination_lon, delivery_charge) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
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
                        "delivery_charge = EXCLUDED.delivery_charge ",
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

        // insert into pay_per_destination table
        deliveryStream.map(
                        delivery -> {
                            Date deliveryDate = Date.valueOf(delivery.getDeliveryDate().toLocalDateTime().toLocalDate());
                            StringTokenizer tokens = new StringTokenizer(delivery.getDeliveryDestination(), " ");
                            String deliveryDestination = tokens.nextToken() + " " + tokens.nextToken();
                            BigDecimal foodPrice = BigDecimal.valueOf(delivery.getFoodPrice());
                            BigDecimal deliveryCharge = BigDecimal.valueOf(delivery.getDeliveryCharge());

                            return new PayPerDestination(deliveryDate, deliveryDestination, foodPrice, deliveryCharge);
                        }
                ).keyBy(new KeySelector<PayPerDestination, Tuple2<Date, String>>() {
                    @Override
                    public Tuple2<Date, String> getKey(PayPerDestination payPerDestination) {
                        return Tuple2.of(payPerDestination.getDeliveryDate(), payPerDestination.getDeliveryDestination());
                    }
                })
                .reduce((t1, payPerDestination) -> {
                    t1.setTotalFoodPrice(payPerDestination.getTotalFoodPrice().add(t1.getTotalFoodPrice()));
                    t1.setTotalDeliveryCharge(payPerDestination.getTotalDeliveryCharge().add(t1.getTotalDeliveryCharge()));

                    return t1;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO pay_per_destination (delivery_date, delivery_destination, total_food_price, total_delivery_charge) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (delivery_date, delivery_destination) DO UPDATE SET " +
                                "total_food_price = EXCLUDED.total_food_price, " +
                                "total_delivery_charge = EXCLUDED.total_delivery_charge",
                        (JdbcStatementBuilder<PayPerDestination>) (preparedStatement, payPerDestination) -> {
                            preparedStatement.setDate(1, payPerDestination.getDeliveryDate());
                            preparedStatement.setString(2, payPerDestination.getDeliveryDestination());
                            preparedStatement.setBigDecimal(3, payPerDestination.getTotalFoodPrice());
                            preparedStatement.setBigDecimal(4, payPerDestination.getTotalDeliveryCharge());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into pay_per_destination table");

        // insert into charge_per_day table
        deliveryStream.map(
                        delivery -> {
                            LocalDate date = delivery.getDeliveryDate().toLocalDateTime().toLocalDate();
                            int month = date.getMonthValue();
                            String day = date.getDayOfWeek().toString().toLowerCase();
                            BigDecimal totalDeliveryCharge = BigDecimal.valueOf(delivery.getDeliveryCharge());

                            return new ChargePerDay(month, day, totalDeliveryCharge);
                        }
                ).keyBy(new KeySelector<ChargePerDay, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> getKey(ChargePerDay chargePerDay) {
                        return Tuple2.of(chargePerDay.getMonth(), chargePerDay.getDay());
                    }
                })
                .reduce((t1, chargePerDay) -> {
                    t1.setTotalDeliveryCharge(chargePerDay.getTotalDeliveryCharge().add(t1.getTotalDeliveryCharge()));

                    return t1;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO charge_per_day (month, day, total_delivery_charge) " +
                                "VALUES (?, ?, ?) " +
                                "ON CONFLICT (month, day) DO UPDATE SET " +
                                "total_delivery_charge = EXCLUDED.total_delivery_charge",
                        (JdbcStatementBuilder<ChargePerDay>) (preparedStatement, chargePerDay) -> {
                            preparedStatement.setInt(1, chargePerDay.getMonth());
                            preparedStatement.setString(2, chargePerDay.getDay());
                            preparedStatement.setBigDecimal(3, chargePerDay.getTotalDeliveryCharge());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into charge_per_day table");

        // insert into pay_per_category
        deliveryStream
                .map(delivery -> {
                            Date deliveryDate = Date.valueOf(delivery.getDeliveryDate().toLocalDateTime().toLocalDate());
                            String foodCategory = delivery.getFoodCategory();
                            BigDecimal totalFoodPrice = BigDecimal.valueOf(delivery.getFoodPrice());

                            return new PayPerCategory(deliveryDate, foodCategory, totalFoodPrice);
                        }
                ).keyBy(new KeySelector<PayPerCategory, Tuple2<Date, String>>() {
                    @Override
                    public Tuple2<Date, String> getKey(PayPerCategory payPerCategory) {
                        return Tuple2.of(payPerCategory.getDeliveryDate(), payPerCategory.getFoodCategory());
                    }
                })
                .reduce((t1, payPerCategory) -> {
                    t1.setTotalFoodPrice(payPerCategory.getTotalFoodPrice().add(t1.getTotalFoodPrice()));

                    return t1;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO pay_per_category (delivery_date, food_category, total_food_price) " +
                                "VALUES(?, ?, ?) " +
                                "ON CONFLICT (delivery_date, food_category) DO UPDATE SET " +
                                "total_food_price = EXCLUDED.total_food_price",
                        (JdbcStatementBuilder<PayPerCategory>) (preparedStatement, payPerCategory) -> {
                            preparedStatement.setDate(1, payPerCategory.getDeliveryDate());
                            preparedStatement.setString(2, payPerCategory.getFoodCategory());
                            preparedStatement.setBigDecimal(3, payPerCategory.getTotalFoodPrice());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into pay_per_category");

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
