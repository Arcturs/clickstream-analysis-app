package ru.spb.itmo.pirsbd.asashina.flinkjobs;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class FlinkJobsApplicationTests {

    @Test
    void contextLoads() {
    }

}
