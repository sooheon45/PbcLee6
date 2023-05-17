package springsaga.common;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import springsaga.OrderApplication;

@CucumberContextConfiguration
@SpringBootTest(classes = { OrderApplication.class })
public class CucumberSpingConfiguration {}
