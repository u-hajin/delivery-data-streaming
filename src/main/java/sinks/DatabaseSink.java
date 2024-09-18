package sinks;

import dto.ChargePerDay;
import dto.Delivery;
import dto.PayPerCategory;
import dto.PayPerDestination;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;


public class DatabaseSink {
    private final JdbcExecutionOptions executionOptions;
    private final JdbcConnectionOptions connectionOptions;

    public DatabaseSink(Properties prop) {
        this.executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        this.connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(prop.getProperty("JDBC_URL"))
                .withDriverName("org.postgresql.Driver")
                .withUsername(prop.getProperty("USER_NAME"))
                .withPassword(prop.getProperty("PASSWORD"))
                .build();
    }

    public void setupDatabaseSinks(DataStream<Delivery> deliveryStream) {
        List<String> createTableStatements = List.of(
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
        );

        createTables(deliveryStream, createTableStatements);

        setupDeliveryInformationSink(deliveryStream);
        setupPayPerDestinationSink(deliveryStream);
        setupChargePerDaySink(deliveryStream);
        setupPayPerCategorySink(deliveryStream);
    }

    private void createTables(DataStream<Delivery> deliveryStream, List<String> createTableStatements) {
        for (String createTableStatement : createTableStatements) {
            deliveryStream.addSink(JdbcSink.sink(
                    createTableStatement,
                    (JdbcStatementBuilder<Delivery>) (preparedStatement, delivery) -> {
                    },
                    this.executionOptions,
                    this.connectionOptions
            ));
        }
    }

    private void setupDeliveryInformationSink(DataStream<Delivery> deliveryStream) {
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
                this.executionOptions,
                this.connectionOptions
        )).name("Insert into delivery_information table");
    }

    private void setupPayPerDestinationSink(DataStream<Delivery> deliveryStream) {
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
                        this.executionOptions,
                        this.connectionOptions
                )).name("Insert into pay_per_destination table");
    }

    private void setupChargePerDaySink(DataStream<Delivery> deliveryStream) {
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
                        this.executionOptions,
                        this.connectionOptions
                )).name("Insert into charge_per_day table");
    }

    private void setupPayPerCategorySink(DataStream<Delivery> deliveryStream) {
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
                        this.executionOptions,
                        this.connectionOptions
                )).name("Insert into pay_per_category");
    }
}
