package springsaga.external;

import java.util.Date;
import lombok.Data;

@Data
public class Order {

    private Long id;
    private String productName;
    private String productId;
    private String status;
    private Integer qty;
    private String userId;
}
