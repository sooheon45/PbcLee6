package springsaga.domain;

import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;
import org.springframework.context.ApplicationContext;
import springsaga.OrderApplication;
import springsaga.domain.OrderPlaced;

@Entity
@Table(name = "Order_table")
@Data
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String productName;

    private String productId;

    private String status;

    private Integer qty;

    private String userId;

    @PostPersist
    public void onPostPersist() {
        OrderPlaced orderPlaced = new OrderPlaced(this);
        orderPlaced.publishAfterCommit();
    }

    public static OrderRepository repository() {
        OrderRepository orderRepository = applicationContext()
            .getBean(OrderRepository.class);
        return orderRepository;
    }

    public static ApplicationContext applicationContext() {
        return OrderApplication.applicationContext;
    }

    public void updateStatus(UpdateStatusCommand updateStatusCommand) {
        // implement the business logics here:

        OrderCompleted orderCompleted = new OrderCompleted(this);
        orderCompleted.setId(updateStatusCommand.getId());
        /** Logic **/

        orderCompleted.publishAfterCommit();
    }

    public void orderCancel(OrderCancelCommand orderCancelCommand) {
        // implement the business logics here:

        OrderCanceled orderCanceled = new OrderCanceled(this);
        orderCanceled.setId(orderCancelCommand.getId());
        /** Logic **/

        orderCanceled.publishAfterCommit();
    }
}
