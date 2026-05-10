package com.example.pyshia;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@ConfigurationPropertiesScan
@SpringBootApplication
public class PyshiaApplication {

    public static void main(String[] args) {
        SpringApplication.run(PyshiaApplication.class, args);
    }

}
