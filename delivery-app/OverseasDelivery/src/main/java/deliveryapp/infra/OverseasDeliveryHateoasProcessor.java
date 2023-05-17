package deliveryapp.infra;

import deliveryapp.domain.*;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.server.RepresentationModelProcessor;
import org.springframework.stereotype.Component;

@Component
public class OverseasDeliveryHateoasProcessor
    implements RepresentationModelProcessor<EntityModel<OverseasDelivery>> {

    @Override
    public EntityModel<OverseasDelivery> process(
        EntityModel<OverseasDelivery> model
    ) {
        return model;
    }
}
