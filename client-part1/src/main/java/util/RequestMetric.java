package util;

public class RequestMetric {
    private final long startTime;
    private final long latency;
    private final int statusCode;
    private final int resortId;

    public RequestMetric(long startTime, long latency, int statusCode, int resortId){
        this.startTime = startTime;
        this.latency = latency;
        this.statusCode = statusCode;
        this.resortId = resortId;
    }

    public String getRequestType() {
        return "SWIPE";
    }

    public int getResortId() {
        return resortId;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getLatency() {
        return latency;
    }

    public int getStatusCode() {
        return statusCode;
    }



}
