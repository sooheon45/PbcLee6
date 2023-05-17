package manufacture.infra;

import manufacture.domain.*;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.server.RepresentationModelProcessor;
import org.springframework.stereotype.Component;

@Component
public class FactoryHateoasProcessor
    implements RepresentationModelProcessor<EntityModel<Factory>> {

    @Override
    public EntityModel<Factory> process(EntityModel<Factory> model) {
        model.add(
            Link
                .of(model.getRequiredLink("self").getHref() + "/recall")
                .withRel("recall")
        );

        return model;
    }
}
