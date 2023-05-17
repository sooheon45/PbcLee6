package springsaga.external;

import java.util.Date;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import springsaga.domain.*;

@FeignClient(name = "Factory", url = "${api.url.Factory}")
public interface FactoryService {
    @RequestMapping(method = RequestMethod.POST, path = "/factories")
    public void makeProduct(@RequestBody Factory factory);
}
