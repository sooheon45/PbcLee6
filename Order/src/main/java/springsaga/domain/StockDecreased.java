package springsaga.domain;

import java.util.*;
import lombok.*;
import springsaga.domain.*;
import springsaga.infra.AbstractEvent;

@Data
@ToString
public class StockDecreased extends AbstractEvent {

    private Long id;
    private String qty;
    private String name;
    private Integer stock;
    private Long orderId;
}
