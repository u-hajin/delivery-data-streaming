package dto;

import lombok.Getter;

import java.sql.Timestamp;

@Getter
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
