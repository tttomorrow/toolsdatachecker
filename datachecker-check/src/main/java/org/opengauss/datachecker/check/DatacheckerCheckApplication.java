package org.opengauss.datachecker.check;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableAsync;


@EnableAsync
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.check.client"})
@SpringBootApplication
public class DatacheckerCheckApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatacheckerCheckApplication.class, args);
    }

}
