package storeapp.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;

@Data
public class DecreaseStockCommand {

    @Id
    //@GeneratedValue(strategy=GenerationType.AUTO)
    private Long id;

    private String name;
    private Integer stock;
    private Long orderId;
}
