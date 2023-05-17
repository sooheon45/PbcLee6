package storeapp.common;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import storeapp.StorageApplication;

@CucumberContextConfiguration
@SpringBootTest(classes = { StorageApplication.class })
public class CucumberSpingConfiguration {}
