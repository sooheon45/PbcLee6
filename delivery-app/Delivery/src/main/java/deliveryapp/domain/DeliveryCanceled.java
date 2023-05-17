package deliveryapp.domain;

import deliveryapp.domain.*;
import deliveryapp.infra.AbstractEvent;
import java.util.*;
import lombok.*;

@Data
@ToString
public class DeliveryCanceled extends AbstractEvent {

    private Long id;
    private String address;
    private String userId;
    private Integer qty;
    private Long orderId;

    public DeliveryCanceled(Delivery aggregate) {
        super(aggregate);
    }

    public DeliveryCanceled() {
        super();
    }
}
