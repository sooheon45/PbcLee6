package storeapp.domain;

import java.util.*;
import lombok.*;
import storeapp.domain.*;
import storeapp.infra.AbstractEvent;

@Data
@ToString
public class StockDecreased extends AbstractEvent {

    private Long id;
    private String qty;
    private String name;
    private Integer stock;
    private Long orderId;

    public StockDecreased(Storage aggregate) {
        super(aggregate);
    }

    public StockDecreased() {
        super();
    }
}
