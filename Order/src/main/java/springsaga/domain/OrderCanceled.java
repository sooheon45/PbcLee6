package springsaga.domain;

import java.util.*;
import lombok.*;
import springsaga.domain.*;
import springsaga.infra.AbstractEvent;

@Data
@ToString
public class OrderCanceled extends AbstractEvent {

    private Long id;
    private String productName;
    private String productId;
    private String status;
    private Integer qty;
    private String userId;

    public OrderCanceled(Order aggregate) {
        super(aggregate);
    }

    public OrderCanceled() {
        super();
    }
}
