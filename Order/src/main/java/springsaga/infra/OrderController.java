package springsaga.infra;

import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springsaga.domain.*;

@RestController
// @RequestMapping(value="/orders")
@Transactional
public class OrderController {

    @Autowired
    OrderRepository orderRepository;

    @RequestMapping(
        value = "orders/{id}/updatestatus",
        method = RequestMethod.PUT,
        produces = "application/json;charset=UTF-8"
    )
    public Order updateStatus(
        @PathVariable(value = "id") Long id,
        @RequestBody UpdateStatusCommand updateStatusCommand,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws Exception {
        System.out.println("##### /order/updateStatus  called #####");
        Optional<Order> optionalOrder = orderRepository.findById(id);

        optionalOrder.orElseThrow(() -> new Exception("No Entity Found"));
        Order order = optionalOrder.get();
        order.updateStatus(updateStatusCommand);

        orderRepository.save(order);
        return order;
    }

    @RequestMapping(
        value = "orders/{id}/ordercancel",
        method = RequestMethod.PUT,
        produces = "application/json;charset=UTF-8"
    )
    public Order orderCancel(
        @PathVariable(value = "id") Long id,
        @RequestBody OrderCancelCommand orderCancelCommand,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws Exception {
        System.out.println("##### /order/orderCancel  called #####");
        Optional<Order> optionalOrder = orderRepository.findById(id);

        optionalOrder.orElseThrow(() -> new Exception("No Entity Found"));
        Order order = optionalOrder.get();
        order.orderCancel(orderCancelCommand);

        orderRepository.save(order);
        return order;
    }
}
