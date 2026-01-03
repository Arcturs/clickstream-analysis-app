package ru.spb.itmo.pirsbd.asashina.flinkjobs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkCassandraDwhProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkCassandraDwhCleanProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkKafkaSourceProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkMinioSourceProperties;

@SpringBootApplication
@EnableConfigurationProperties({
        FlinkMinioSourceProperties.class,
        FlinkCassandraDwhProperties.class,
        FlinkKafkaSourceProperties.class,
        FlinkCassandraDwhCleanProperties.class
})
public class FlinkJobsApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkJobsApplication.class, args);
    }

}
