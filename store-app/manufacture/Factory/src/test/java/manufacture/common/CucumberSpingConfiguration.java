package manufacture.common;

import io.cucumber.spring.CucumberContextConfiguration;
import manufacture.FactoryApplication;
import org.springframework.boot.test.context.SpringBootTest;

@CucumberContextConfiguration
@SpringBootTest(classes = { FactoryApplication.class })
public class CucumberSpingConfiguration {}
