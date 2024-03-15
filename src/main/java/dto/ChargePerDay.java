package dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@AllArgsConstructor
@Data
public class ChargePerDay {
    private String day;
    private BigDecimal totalDeliveryCharge;
}
