package springsaga.external;

import java.util.Date;
import lombok.Data;

@Data
public class OverseasDelivery {

    private Long id;
    private String address;
    private String countryCode;
    private String userId;
    private String qty;
    private Long orderId;
}
