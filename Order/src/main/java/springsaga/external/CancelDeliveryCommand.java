package springsaga.external;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;

@Data
public class CancelDeliveryCommand {

    @Id
    private Long id;

    private Long orderId;
}
