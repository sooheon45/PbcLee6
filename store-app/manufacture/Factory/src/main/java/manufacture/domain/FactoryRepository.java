package manufacture.domain;

import manufacture.domain.*;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(collectionResourceRel = "factories", path = "factories")
public interface FactoryRepository
    extends PagingAndSortingRepository<Factory, Long> {}
