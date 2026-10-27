import model.SwipeGen;
import org.apache.http.client.config.RequestConfig;
import util.ClientConfig;
import util.RequestMetric;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;

public class IngestPath {
    private static final int seasonId = 2026;
    private static final int dayId = 1;
    private static final int MAX_RETRIES = 5;
    private static final int RESORT_SIZE = 10;
    private static final int STATUS_CODE_SUCCESS = 201;
    private static final int WARMUP_THREADS = 32;
    private static final int WARMUP_POSTS = 1000;
    private static final int SKIER_ID_MAX = 100000;
    private static final int RESORT_ID_MAX = 10;
    private static final int LIFT_ID_MAX = 40;
    private static final int TIME_MAX = 360;
    private static final int DEFAULT_DELAY = 100;
    private static final int DELAY_MULTIPLIER = 2;
    private static final int MAX_TIMEOUT_TIME = 10000;
    private static final Random rand = new Random();
    private static final String[][] cachedJson = new String[361][41];
    static{
        for(int i = 1; i <= 360; i++){
            for(int j = 1; j <= 40; j++){
                cachedJson[i][j] = "{\"time\":" + i + ",\"liftID\":" + j + "}";
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ClientConfig config;
        try{
            config = ClientConfig.fromArgs(args);
        } catch (IllegalArgumentException e){
            System.err.println(e.getMessage());
            return;
        }
        int maxConnections = config.MAIN_THREADS;;
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager();
        pool.setMaxTotal(maxConnections);
        pool.setDefaultMaxPerRoute(maxConnections);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(MAX_TIMEOUT_TIME)
                .setSocketTimeout(MAX_TIMEOUT_TIME).setConnectionRequestTimeout(MAX_TIMEOUT_TIME).build();
        try (CloseableHttpClient client = HttpClients.custom()
                .setConnectionManager(pool).setDefaultRequestConfig(requestConfig).disableAutomaticRetries().build()) {
            LeaderboardSubscriber[] subscriber = new LeaderboardSubscriber[RESORT_SIZE];
            long warmUpTime = 0;
            long totalTime = 0;
            try {
                for (int i = 0; i < RESORT_SIZE; i++) {
                    subscriber[i] = new LeaderboardSubscriber();
                    subscriber[i].connect(config.WS_BASE_URL,i + 1);
                }
                long start = System.currentTimeMillis();
                run(client, WARMUP_THREADS, WARMUP_POSTS, "warmup_metric.csv", config.HTTP_BASE_URL);
                warmUpTime = System.currentTimeMillis() - start;
                run(client, config.MAIN_THREADS, config.MAIN_POSTS, "main_metrics.csv", config.HTTP_BASE_URL);
                totalTime = System.currentTimeMillis() - start;
            } finally {
                for (LeaderboardSubscriber s : subscriber) {
                    if (s != null) {
                        s.disconnect();
                    }
                }
            }
            int totalUpdates = 0, connectionCount = 0, reconnCount = 0;
            long time = 0;
            for (int i = 0; i < RESORT_SIZE; i++) {
                LeaderboardSubscriber s = subscriber[i];
                int count = s.getMessageCount(), conn = s.getConnectionCount(), reconn = s.getReconnectionCount();
                long currTime = s.getTime();
                connectionCount += conn;
                reconnCount += reconn;
                System.out.println("Resort " + (i + 1) + " got " + count + " leaderboard updates, "
                        + "connected count is " + conn + ", reconnected count is " + reconn
                        + ", time for connection is " + currTime + " ms");
                totalUpdates += count;
                time += currTime;
            }
            System.out.println("For the HTTP ingest Swipe POST:");
            System.out.println("The WarmUp runtime is " + warmUpTime + " ms.");
            System.out.println("Total wall-clock runtime is " + totalTime + " ms.");
            System.out.println("For WebSocket subscribe:");
            System.out.println("Leaderboard received " + totalUpdates + " updates in total, "
                    + "the total number of connected is " + connectionCount + ", total number of reconnected is "
                    + reconnCount + ".");
            System.out.println("And total time for connection establish is " + time + " ms.");
        }
    }

    private static void run(CloseableHttpClient client, int numThreads, int numPosts, String fileName, String url)
            throws InterruptedException{
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger waited = new AtomicInteger(0);
        AtomicInteger generatedCount = new AtomicInteger(0);
        AtomicLong firstConsumedTime = new AtomicLong(0);
        AtomicLong producerFinishedTime = new AtomicLong(0);
        AtomicBoolean producerDone = new AtomicBoolean(false);
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        LongAdder totalAttempts = new LongAdder();
        LongAdder connectionResets = new LongAdder();
        LongAdder readTimeouts = new LongAdder();
        LongAdder otherIOErrors = new LongAdder();
        LongAdder retriedRequests = new LongAdder();
        List<List<RequestMetric>> metrics = new ArrayList<>(numThreads);
        for(int i = 0; i < numThreads; i++){
            metrics.add(new ArrayList<>(numPosts));
        }

        int totalPosts = numThreads * numPosts;
        CountDownLatch threadsDown = new CountDownLatch(numThreads);
        CountDownLatch ready = new CountDownLatch(numThreads);
        CountDownLatch startGate = new CountDownLatch(1);
        ConcurrentLinkedQueue<SwipeGen> queue = new ConcurrentLinkedQueue<>();

        Thread producer = new Thread(() -> {
            try {
                startGate.await();
                long genStart = System.currentTimeMillis();
                for (int i = 0; i < totalPosts; i++) {
                    SwipeGen swipeGen = randomGen();
                    queue.offer(swipeGen);
                    generatedCount.incrementAndGet();
                }
                long genTime = System.currentTimeMillis() - genStart;
                System.out.println("\nSwipe Generation time: " + genTime + "ms");
            } catch (InterruptedException e) {
                producerError.set(e);
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                producerError.set(e);
            } finally {
                producerFinishedTime.set(System.nanoTime());
                producerDone.set(true);
            }
        });
        producer.start();

        for (int i = 0; i < numThreads; i++) {
            List<RequestMetric> localMetrics = metrics.get(i);
            new Thread(() -> {
                try {
                    ready.countDown();
                    startGate.await();
                    for (int j = 0; j < numPosts;) {
                        SwipeGen top = queue.poll();
                        if (top == null) {
                            if(producerDone.get()){
                                if(producerError.get() != null){
                                    System.err.println("Producer failed: " + producerError.get());
                                } else{
                                    System.err.println("Producer finished, but expected swipe is missing.");
                                }
                                break;
                            }
                            waited.incrementAndGet();
                            Thread.yield();
                            continue;
                        }
                        firstConsumedTime.compareAndSet(0, System.nanoTime());
                        String jsonStr = cachedJson[top.getTime()][top.getLiftID()];
                        HttpPost post = new HttpPost(urlGen(top, url));
                        post.setEntity(new StringEntity(jsonStr, ContentType.APPLICATION_JSON));
                        try {
                            boolean sent = retry(client, post, localMetrics, top.getResortID(),
                                    totalAttempts, connectionResets, readTimeouts, otherIOErrors, retriedRequests);
                            if (sent) {
                                success.incrementAndGet();
                            } else {
                                failed.incrementAndGet();
                            }

                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            failed.incrementAndGet();
                            break;
                        }
                        j++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    threadsDown.countDown();
                }
            }).start();
        }
        try {
            ready.await();
            long sendStart = System.currentTimeMillis();
            startGate.countDown();
            producer.join();
            System.out.println("Generated events count is " + generatedCount.get());
            threadsDown.await();
            boolean overlapped = firstConsumedTime.get() > 0 && firstConsumedTime.get() < producerFinishedTime.get();
            System.out.println("Producer-Consumer overlap: " + overlapped); //Check for concurrency

            if(producerError.get() != null){
                throw new IllegalStateException("Producer failed to generate all swipe events", producerError.get());
            }
            if(generatedCount.get() != totalPosts){
                throw new IllegalStateException("Expected " + totalPosts +
                        " posts, but generated " + generatedCount.get());
            }

            int accounted = success.get() + failed.get();
            if(accounted != totalPosts){
                throw new IllegalStateException("Incomplete benchmark: expected " + totalPosts
                        + " requests, but processed " + accounted);
            }

            long elapsed = System.currentTimeMillis() - sendStart;
            System.out.printf("Concurrent ingest phase throughput: %.2f req/s%n",
                    (double) totalPosts * 1000 / elapsed);

            List<RequestMetric> allMetrics = new ArrayList<>(totalPosts);
            for(List<RequestMetric> i:metrics){
                allMetrics.addAll(i);
            }
            try (PrintWriter writer = new PrintWriter(fileName)) {
                writer.println("timestamp,requestType,latency,statusCode,resortID");
                for (RequestMetric metric : allMetrics) {
                    writer.println(metric.getStartTime() + "," + metric.getRequestType() + "," +
                            metric.getLatency() + "," + metric.getStatusCode() + "," + metric.getResortId());
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Expected number of posts will be receive: " + totalPosts);
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }

        System.out.println("For " + numThreads + " threads, successfully sent out " + success.get() +
                " SWIPE posts, number of failed SWIPE posts is " + failed.get()
                + ". Number of empty queue poll: " + waited.get());
        System.out.println("\nHTTP Reliability Summary:");
        System.out.print("Total logical requests: " + totalPosts + ". Actual HTTP attempts: " + totalAttempts.sum() +
                ". Connection resets: " + connectionResets.sum() + ". Timeout errors: " + readTimeouts.sum() +
                ". Other IO errors: " + otherIOErrors.sum()
                + ". Retried logical requests: " + retriedRequests.sum() + "\n");
    }

    private static String urlGen(SwipeGen swipeGen, String url){
        return url + "/skiers/" + swipeGen.getResortID() + "/seasons/" +
                swipeGen.getSeasonID() + "/days/" + swipeGen.getDayID() + "/skiers/" +
                swipeGen.getSkierID();
    }

    private static SwipeGen randomGen(){
        //[min,max] = (max - min + 1) + min
        int skierId = rand.nextInt(SKIER_ID_MAX) + 1;
        int resortId = rand.nextInt(RESORT_ID_MAX) + 1;
        int liftId = rand.nextInt(LIFT_ID_MAX) + 1;
        int time = rand.nextInt(TIME_MAX) + 1;
        return new SwipeGen(skierId,resortId,liftId,seasonId,dayId,time);
    }

    private static boolean retry(CloseableHttpClient client, HttpPost post, List<RequestMetric> metrics, int resortId,
                                 LongAdder totalAttempts, LongAdder connectionResets, LongAdder readTimeouts,
                                 LongAdder otherIOErrors, LongAdder retriedRequests) throws InterruptedException{
        int delay = DEFAULT_DELAY;
        for (int i = 0; i <= MAX_RETRIES; i++) {
            if(i == 1){
                retriedRequests.increment();
            }
            try {
                long start = System.currentTimeMillis();
                totalAttempts.increment();
                int statusCode;
                try (org.apache.http.client.methods.CloseableHttpResponse response = client.execute(post)) {
                    statusCode = response.getStatusLine().getStatusCode();
                    // Consume response body so the connection can be reused.
                    EntityUtils.consume(response.getEntity());
                }
                long latency = System.currentTimeMillis() - start;
                metrics.add(new RequestMetric(start, latency, statusCode, resortId));
                if (statusCode == STATUS_CODE_SUCCESS) {
                    return true;
                }
            } catch (IOException e) {
                String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
                if(message.contains("connection reset")) connectionResets.increment();
                else if(message.contains("timed out") || message.contains("timeout")) readTimeouts.increment();
                else otherIOErrors.increment();
            }
            if(i < MAX_RETRIES) {
                Thread.sleep(delay);
                delay *= DELAY_MULTIPLIER;
            }
        }
        return false;
    }

}
