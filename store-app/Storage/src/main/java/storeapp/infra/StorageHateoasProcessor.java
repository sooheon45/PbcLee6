package storeapp.infra;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.server.RepresentationModelProcessor;
import org.springframework.stereotype.Component;
import storeapp.domain.*;

@Component
public class StorageHateoasProcessor
    implements RepresentationModelProcessor<EntityModel<Storage>> {

    @Override
    public EntityModel<Storage> process(EntityModel<Storage> model) {
        model.add(
            Link
                .of(model.getRequiredLink("self").getHref() + "/decreasestock")
                .withRel("decreasestock")
        );

        return model;
    }
}
