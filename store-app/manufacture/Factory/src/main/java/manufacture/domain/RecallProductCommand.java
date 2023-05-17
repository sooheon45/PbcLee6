package manufacture.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;

@Data
public class RecallProductCommand {

    @Id
    //@GeneratedValue(strategy=GenerationType.AUTO)
    private Long id;

    private String lineNum;
    private Integer qty;
    private Integer name;
    private Long recallId;
}
