package springsaga.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;

@Data
public class UpdateStatusCommand {

    @Id
    //@GeneratedValue(strategy=GenerationType.AUTO)
    private Long id;

    private String productName;
    private String productId;
    private String status;
    private Integer qty;
    private String userId;
}
