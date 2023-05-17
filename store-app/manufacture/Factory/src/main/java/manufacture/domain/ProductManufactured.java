package manufacture.domain;

import java.util.*;
import lombok.*;
import manufacture.domain.*;
import manufacture.infra.AbstractEvent;

@Data
@ToString
public class ProductManufactured extends AbstractEvent {

    private Long id;
    private String lineNum;
    private Integer qty;
    private Integer name;
    private Long orderId;

    public ProductManufactured(Factory aggregate) {
        super(aggregate);
    }

    public ProductManufactured() {
        super();
    }
}
