package deliveryapp.infra;

import deliveryapp.domain.*;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
// @RequestMapping(value="/deliveries")
@Transactional
public class DeliveryController {

    @Autowired
    DeliveryRepository deliveryRepository;

    @RequestMapping(
        value = "deliveries/{id}/canceldelivery",
        method = RequestMethod.PUT,
        produces = "application/json;charset=UTF-8"
    )
    public Delivery cancelDelivery(
        @PathVariable(value = "id") Long id,
        @RequestBody CancelDeliveryCommand cancelDeliveryCommand,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws Exception {
        System.out.println("##### /delivery/cancelDelivery  called #####");
        Optional<Delivery> optionalDelivery = deliveryRepository.findById(id);

        optionalDelivery.orElseThrow(() -> new Exception("No Entity Found"));
        Delivery delivery = optionalDelivery.get();
        delivery.cancelDelivery(cancelDeliveryCommand);

        deliveryRepository.save(delivery);
        return delivery;
    }

    @RequestMapping(
        value = "deliveries/{id}/returndelivery",
        method = RequestMethod.PUT,
        produces = "application/json;charset=UTF-8"
    )
    public Delivery returnDelivery(
        @PathVariable(value = "id") Long id,
        @RequestBody ReturnDeliveryCommand returnDeliveryCommand,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws Exception {
        System.out.println("##### /delivery/returnDelivery  called #####");
        Optional<Delivery> optionalDelivery = deliveryRepository.findById(id);

        optionalDelivery.orElseThrow(() -> new Exception("No Entity Found"));
        Delivery delivery = optionalDelivery.get();
        delivery.returnDelivery(returnDeliveryCommand);

        deliveryRepository.save(delivery);
        return delivery;
    }
}
