package springsaga.external;

import java.util.Date;
import lombok.Data;

@Data
public class Delivery {

    private Long id;
    private String address;
    private String userId;
    private Integer qty;
    private Long orderId;
}
