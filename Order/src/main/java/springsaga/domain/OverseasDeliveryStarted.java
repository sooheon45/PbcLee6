package springsaga.domain;

import java.util.*;
import lombok.*;
import springsaga.domain.*;
import springsaga.infra.AbstractEvent;

@Data
@ToString
public class OverseasDeliveryStarted extends AbstractEvent {

    private Long id;
    private String address;
    private String countryCode;
    private String userId;
    private String qty;
    private Long orderId;
}
