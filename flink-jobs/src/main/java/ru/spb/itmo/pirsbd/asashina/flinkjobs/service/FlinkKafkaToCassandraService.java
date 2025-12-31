package ru.spb.itmo.pirsbd.asashina.flinkjobs.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.FlinkKafkaToCassandraProcessor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class FlinkKafkaToCassandraService {

    private final FlinkKafkaToCassandraProcessor processor;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public FlinkKafkaToCassandraService(FlinkKafkaToCassandraProcessor processor) {
        this.processor = processor;
    }

    @PostConstruct
    public void execute() {
        executorService.execute(() -> {
            try {
                processor.process();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

}
