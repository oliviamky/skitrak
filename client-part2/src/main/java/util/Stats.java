
package util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Stats {
    private static final int STATUS_CODE_SUCCESS = 201;
    private static final double THROUGHPUT_MULTIPLIER = 1000.0;
    private static final double PERCENTILE_NINETY_FIVE = 0.95;
    private static final double PERCENTILE_NINETY_NINE = 0.99;
    private Stats() {}

    public static void printHttpLatency(List<RequestMetric> metrics) {
        List<Long> latencies = new ArrayList<>();
        for (RequestMetric metric : metrics) {
            latencies.add(metric.getLatency());
        }
        printLatency("HTTP Ingest", latencies);
    }

    public static void printPushLatency(List<Long> latencies) {
        printLatency("WebSocket Push", latencies);
    }

    private static void printLatency(String name, List<Long> values) {
        if (values.isEmpty()) {
            System.out.println(name + ": No latency samples.");
            return;
        }
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        double mean = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        double median = n % 2 == 0 ? (sorted.get(n / 2 - 1) + (double) sorted.get(n / 2)) / 2 : sorted.get(n / 2);
        long p95 = percentile(sorted, PERCENTILE_NINETY_FIVE);
        long p99 = percentile(sorted, PERCENTILE_NINETY_NINE);
        System.out.printf("%s Latency: Sample size: %d, Mean: %.4f ms, Median: %.4f ms, "
                        + "p95: %d ms, p99: %d ms, Min: %d ms, Max: %d ms%n%n",
                name, n, mean, median, p95, p99, sorted.get(0), sorted.get(n - 1));
    }

    private static long percentile(List<Long> sorted, double p) {
        int index = (int) Math.ceil(sorted.size() * p) - 1;
        return sorted.get(Math.max(0, index));
    }

    // Throughput for each resort
    public static void printThroughputPerResort(List<RequestMetric> metrics, long elapsedMs, int resortCount) {
        int[] counts = new int[resortCount];
        for (RequestMetric metric : metrics) {
            int resortId = metric.getResortId();
            if (metric.getStatusCode() == STATUS_CODE_SUCCESS && resortId >= 1 && resortId <= resortCount) {
                counts[resortId - 1]++;
            }
        }
        System.out.println("\nThroughput per Resort:");
        for (int i = 0; i < resortCount; i++) {
            double throughput = counts[i] * THROUGHPUT_MULTIPLIER / elapsedMs;
            System.out.printf("Resort %d: %d swipes, %.2f req/s %n", i + 1, counts[i], throughput);
        }
        System.out.println();
    }

    // Export individual request measurements
    public static void exportCsv(List<RequestMetric> metrics, String fileName) throws IOException {
        try (PrintWriter writer = new PrintWriter(fileName)) {
            writer.println("timestamp,requestType,latency,statusCode,resortID");
            for (RequestMetric metric : metrics) {
                writer.printf("%d,%s,%d,%d,%d%n", metric.getStartTime(), metric.getRequestType(),
                        metric.getLatency(), metric.getStatusCode(), metric.getResortId());
            }
        }
    }

    // Export throughput in 10-second buckets
    // Uses HTTP response completion time.
    public static void exportThroughputBuckets(List<RequestMetric> metrics, long sendingStartMs,
            String fileName) throws IOException {
        final long BUCKET_MS = 10_000;
        List<Integer> counts = new ArrayList<>();
        long latestCompletion = sendingStartMs;
        for (RequestMetric metric : metrics) {
            if (metric.getStatusCode() != STATUS_CODE_SUCCESS) {
                continue;
            }
            long completionTime = metric.getStartTime() + metric.getLatency();
            if (completionTime < sendingStartMs) {
                continue;
            }
            latestCompletion = Math.max(latestCompletion, completionTime);
            int bucket = (int) ((completionTime - sendingStartMs) / BUCKET_MS);
            while (counts.size() <= bucket) {
                counts.add(0);
            }
            counts.set(bucket, counts.get(bucket) + 1);
        }
        try (PrintWriter writer = new PrintWriter(fileName)) {
            writer.println("timeSeconds,throughputReqPerSec");
            int fullBuckets = (int) ((latestCompletion - sendingStartMs) / BUCKET_MS);
            for (int i = 0; i < fullBuckets; i++) {
                double throughput = counts.get(i) / 10.0;
                writer.printf(java.util.Locale.US, "%d,%.2f%n", i * 10, throughput);
            }
        }
    }
}
