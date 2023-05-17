package deliveryapp.domain;

import deliveryapp.domain.*;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(
    collectionResourceRel = "overseasDeliveries",
    path = "overseasDeliveries"
)
public interface OverseasDeliveryRepository
    extends PagingAndSortingRepository<OverseasDelivery, Long> {}
