package springsaga.domain;

import java.util.*;
import lombok.*;
import springsaga.domain.*;
import springsaga.infra.AbstractEvent;

@Data
@ToString
public class DeliveryStarted extends AbstractEvent {

    private Long id;
    private String address;
    private String userId;
    private Integer qty;
    private Long orderId;
}
