package dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.Date;

@AllArgsConstructor
@Data
public class PayPerCategory {
    private Date deliveryDate;
    private String foodCategory;
    private BigDecimal totalFoodPrice;
}
