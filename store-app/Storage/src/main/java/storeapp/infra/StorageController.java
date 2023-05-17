package storeapp.infra;

import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import storeapp.domain.*;

@RestController
// @RequestMapping(value="/storages")
@Transactional
public class StorageController {

    @Autowired
    StorageRepository storageRepository;

    @RequestMapping(
        value = "storages/{id}/decreasestock",
        method = RequestMethod.PUT,
        produces = "application/json;charset=UTF-8"
    )
    public Storage decreaseStock(
        @PathVariable(value = "id") Long id,
        @RequestBody DecreaseStockCommand decreaseStockCommand,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws Exception {
        System.out.println("##### /storage/decreaseStock  called #####");
        Optional<Storage> optionalStorage = storageRepository.findById(id);

        optionalStorage.orElseThrow(() -> new Exception("No Entity Found"));
        Storage storage = optionalStorage.get();
        storage.decreaseStock(decreaseStockCommand);

        storageRepository.save(storage);
        return storage;
    }
}
