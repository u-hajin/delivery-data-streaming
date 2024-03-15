package dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class PayPerDestination {
    private String deliveryDestination;
    private BigDecimal totalFoodPrice;
    private BigDecimal totalDeliveryCharge;

}
