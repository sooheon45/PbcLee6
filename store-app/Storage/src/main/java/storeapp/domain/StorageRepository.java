package storeapp.domain;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import storeapp.domain.*;

@RepositoryRestResource(collectionResourceRel = "storages", path = "storages")
public interface StorageRepository
    extends PagingAndSortingRepository<Storage, Long> {}
