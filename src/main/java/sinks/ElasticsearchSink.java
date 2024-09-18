package sinks;

import application.DataStreamJob;
import com.fasterxml.jackson.core.JsonProcessingException;
import dto.Delivery;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.JsonUtil.convertDeliveryDataToJson;

public class ElasticsearchSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamJob.class);
    private final Elasticsearch7SinkBuilder<Delivery> elasticsearchSinkBuilder;


    public ElasticsearchSink() {
        this.elasticsearchSinkBuilder = new Elasticsearch7SinkBuilder<>()
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
    }

    public void setupElasticsearchSink(DataStream<Delivery> deliveryStream) {
        deliveryStream.sinkTo(
                this.elasticsearchSinkBuilder.build()
        ).name("Elasticsearch sink");
    }
}
