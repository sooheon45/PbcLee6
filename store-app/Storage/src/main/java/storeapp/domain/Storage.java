package storeapp.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;
import org.springframework.context.ApplicationContext;
import storeapp.StorageApplication;
import storeapp.domain.StockIncreased;

@Entity
@Table(name = "Storage_table")
@Data
public class Storage {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String name;

    private Integer stock;

    @PostPersist
    public void onPostPersist() {
        StockIncreased stockIncreased = new StockIncreased(this);
        stockIncreased.publishAfterCommit();
    }

    public static StorageRepository repository() {
        StorageRepository storageRepository = applicationContext()
            .getBean(StorageRepository.class);
        return storageRepository;
    }

    public static ApplicationContext applicationContext() {
        return StorageApplication.applicationContext;
    }

    public void decreaseStock(DecreaseStockCommand decreaseStockCommand) {
        // implement the business logics here:

        StockDecreased stockDecreased = new StockDecreased(this);
        stockDecreased.setOrderId(decreaseStockCommand.getOrderId());
        /** Logic **/

        stockDecreased.publishAfterCommit();
    }
}
