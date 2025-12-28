package ru.spb.itmo.pirsbd.asashina.flinkjobs;

import org.springframework.boot.SpringApplication;

public class TestFlinkJobsApplication {

    public static void main(String[] args) {
        SpringApplication.from(FlinkJobsApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
