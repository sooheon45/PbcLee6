package manufacture.domain;

import java.util.*;
import lombok.*;
import manufacture.domain.*;
import manufacture.infra.AbstractEvent;

@Data
@ToString
public class ProductRecalled extends AbstractEvent {

    private Long id;
    private String lineNum;
    private Integer qty;
    private Integer name;
    private Long orderId;

    public ProductRecalled(Factory aggregate) {
        super(aggregate);
    }

    public ProductRecalled() {
        super();
    }
}
