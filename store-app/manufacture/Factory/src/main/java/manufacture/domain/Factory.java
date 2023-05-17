package manufacture.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;
import manufacture.FactoryApplication;
import manufacture.domain.ProductManufactured;
import org.springframework.context.ApplicationContext;

@Entity
@Table(name = "Factory_table")
@Data
public class Factory {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String lineNum;

    private Integer qty;

    private Integer name;

    private Long orderId;

    @PostPersist
    public void onPostPersist() {
        ProductManufactured productManufactured = new ProductManufactured(this);
        productManufactured.publishAfterCommit();
    }

    public static FactoryRepository repository() {
        FactoryRepository factoryRepository = applicationContext()
            .getBean(FactoryRepository.class);
        return factoryRepository;
    }

    public static ApplicationContext applicationContext() {
        return FactoryApplication.applicationContext;
    }

    public void recallProduct(RecallProductCommand recallProductCommand) {
        // implement the business logics here:

        ProductRecalled productRecalled = new ProductRecalled(this);
        productRecalled.setOrderId(recallProductCommand.getRecallId());
        /** Logic **/

        productRecalled.publishAfterCommit();
    }
}
