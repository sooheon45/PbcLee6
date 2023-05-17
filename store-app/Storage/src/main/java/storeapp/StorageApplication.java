package storeapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.ApplicationContext;
import storeapp.config.kafka.KafkaProcessor;

@SpringBootApplication
@EnableBinding(KafkaProcessor.class)
@EnableFeignClients
public class StorageApplication {

    public static ApplicationContext applicationContext;

    public static void main(String[] args) {
        applicationContext =
            SpringApplication.run(StorageApplication.class, args);
    }
}
