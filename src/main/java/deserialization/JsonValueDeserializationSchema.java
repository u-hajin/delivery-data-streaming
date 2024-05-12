package deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.Delivery;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonValueDeserializationSchema implements DeserializationSchema<Delivery> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Delivery deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Delivery.class);
    }

    @Override
    public boolean isEndOfStream(Delivery delivery) {
        return false;
    }

    @Override
    public TypeInformation<Delivery> getProducedType() {
        return TypeInformation.of(Delivery.class);
    }
}
