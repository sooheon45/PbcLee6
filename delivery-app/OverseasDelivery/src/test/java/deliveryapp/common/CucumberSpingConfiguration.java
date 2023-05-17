package deliveryapp.common;

import deliveryapp.OverseasDeliveryApplication;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

@CucumberContextConfiguration
@SpringBootTest(classes = { OverseasDeliveryApplication.class })
public class CucumberSpingConfiguration {}
