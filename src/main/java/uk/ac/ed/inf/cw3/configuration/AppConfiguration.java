package uk.ac.ed.inf.cw3.configuration;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import uk.ac.ed.inf.cw3.service.DynamoDbAnalyticsService;
import uk.ac.ed.inf.cw3.service.HealthEventProcessor;
import uk.ac.ed.inf.cw3.service.HealthMonitorService;
import uk.ac.ed.inf.cw3.service.RecoveryService;

@Configuration
public class AppConfiguration {

    private final HealthMonitorService healthMonitorService;
    private final RecoveryService recoveryService;
    private final DynamoDbAnalyticsService analyticsService;
    private final HealthEventProcessor eventProcessor;

    public AppConfiguration(HealthMonitorService healthMonitorService,
                            RecoveryService recoveryService,
                            DynamoDbAnalyticsService analyticsService,
                            HealthEventProcessor eventProcessor) {
        this.healthMonitorService = healthMonitorService;
        this.recoveryService = recoveryService;
        this.analyticsService = analyticsService;
        this.eventProcessor = eventProcessor;
    }

    @PostConstruct
    public void wireServices() {
        healthMonitorService.setRecoveryService(recoveryService);
        healthMonitorService.setAnalyticsService(analyticsService);
        healthMonitorService.setEventProcessor(eventProcessor);
    }
}