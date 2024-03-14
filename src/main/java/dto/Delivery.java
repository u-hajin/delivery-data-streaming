package dto;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Delivery {
    private String deliveryId;
    private Timestamp deliveryDate;
    private String userId;
    private String foodCategory;
    private double foodPrice;
    private String paymentMethod;
    private double deliveryDistance;
    private String deliveryDestination;
    private double destinationLat;
    private double destinationLon;
    private int deliveryCharge;
}
