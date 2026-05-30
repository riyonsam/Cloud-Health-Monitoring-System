package uk.ac.ed.inf.cw3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Cw3Application {

    public static void main(String[] args) {
        SpringApplication.run(Cw3Application.class, args);
    }
}