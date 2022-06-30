package org.opengauss.datachecker.extract;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.extract.client"})
@SpringBootApplication
public class ExtractApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExtractApplication.class, args);
    }

}
