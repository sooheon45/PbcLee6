package deliveryapp.domain;

import deliveryapp.domain.*;
import deliveryapp.infra.AbstractEvent;
import java.util.*;
import lombok.*;

@Data
@ToString
public class OverseasDeliveryStarted extends AbstractEvent {

    private Long id;
    private String address;
    private String countryCode;
    private String userId;
    private String qty;
    private Long orderId;

    public OverseasDeliveryStarted(OverseasDelivery aggregate) {
        super(aggregate);
    }

    public OverseasDeliveryStarted() {
        super();
    }
}
