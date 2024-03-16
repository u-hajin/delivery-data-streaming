package dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.Date;

@Data
@AllArgsConstructor
public class PayPerDestination {
    private Date deliveryDate;
    private String deliveryDestination;
    private BigDecimal totalFoodPrice;
    private BigDecimal totalDeliveryCharge;

}
