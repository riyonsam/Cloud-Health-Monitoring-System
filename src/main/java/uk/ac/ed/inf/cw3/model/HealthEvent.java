package uk.ac.ed.inf.cw3.model;

import java.time.Instant;

public class HealthEvent {

    private String serviceName;
    private ServiceStatus status;
    private String message;
    private long responseTimeMs;
    private Instant timestamp;
    private boolean recoveryAttempted;
    private boolean recoverySuccessful;

    public HealthEvent() {
        this.timestamp = Instant.now();
        this.recoveryAttempted = false;
        this.recoverySuccessful = false;
    }

    public HealthEvent(String serviceName, ServiceStatus status,
                       String message, long responseTimeMs) {
        this();
        this.serviceName = serviceName;
        this.status = status;
        this.message = message;
        this.responseTimeMs = responseTimeMs;
    }

    public String getServiceName()          { return serviceName; }
    public ServiceStatus getStatus()        { return status; }
    public String getMessage()              { return message; }
    public long getResponseTimeMs()         { return responseTimeMs; }
    public Instant getTimestamp()           { return timestamp; }
    public boolean isRecoveryAttempted()    { return recoveryAttempted; }
    public boolean isRecoverySuccessful()   { return recoverySuccessful; }

    public void setServiceName(String serviceName)          { this.serviceName = serviceName; }
    public void setStatus(ServiceStatus status)             { this.status = status; }
    public void setMessage(String message)                  { this.message = message; }
    public void setResponseTimeMs(long responseTimeMs)      { this.responseTimeMs = responseTimeMs; }
    public void setTimestamp(Instant timestamp)             { this.timestamp = timestamp; }
    public void setRecoveryAttempted(boolean attempted)     { this.recoveryAttempted = attempted; }
    public void setRecoverySuccessful(boolean successful)   { this.recoverySuccessful = successful; }
}