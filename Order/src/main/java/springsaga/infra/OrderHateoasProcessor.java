package springsaga.infra;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.server.RepresentationModelProcessor;
import org.springframework.stereotype.Component;
import springsaga.domain.*;

@Component
public class OrderHateoasProcessor
    implements RepresentationModelProcessor<EntityModel<Order>> {

    @Override
    public EntityModel<Order> process(EntityModel<Order> model) {
        model.add(
            Link
                .of(model.getRequiredLink("self").getHref() + "/updatestatus")
                .withRel("updatestatus")
        );
        model.add(
            Link
                .of(model.getRequiredLink("self").getHref() + "/ordercancel")
                .withRel("ordercancel")
        );

        return model;
    }
}
