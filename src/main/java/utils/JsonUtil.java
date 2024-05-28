package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dto.Delivery;

public class JsonUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String convertDeliveryDataToJson(Delivery delivery) throws JsonProcessingException {
        try {
            ObjectNode rootNode = OBJECT_MAPPER.createObjectNode();
            rootNode.put("deliveryId", delivery.getDeliveryId());
            rootNode.put("deliveryDate", delivery.getDeliveryDate().toString());
            rootNode.put("userId", delivery.getUserId());
            rootNode.put("foodCategory", delivery.getFoodCategory());
            rootNode.put("foodPrice", delivery.getFoodPrice());
            rootNode.put("paymentMethod", delivery.getPaymentMethod());
            rootNode.put("deliveryDistance", delivery.getDeliveryDistance());
            rootNode.put("deliveryDestination", delivery.getDeliveryDestination());
            rootNode.put("deliveryCharge", delivery.getDeliveryCharge());

            ObjectNode locationNode = OBJECT_MAPPER.createObjectNode();
            locationNode.put("lat", delivery.getDestinationLat());
            locationNode.put("lon", delivery.getDestinationLon());

            rootNode.set("location", locationNode);

            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
