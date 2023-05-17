package springsaga.domain;

import java.util.*;
import lombok.*;
import springsaga.domain.*;
import springsaga.infra.AbstractEvent;

@Data
@ToString
public class ProductManufactured extends AbstractEvent {

    private Long id;
    private String lineNum;
    private Integer qty;
    private Integer name;
    private Long orderId;
}
