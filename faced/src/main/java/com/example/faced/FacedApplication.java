package com.example.faced;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@SpringBootApplication
@ServletComponentScan
public class FacedApplication {

    public static void main(String[] args) {
        SpringApplication.run(FacedApplication.class, args);

    }

}
