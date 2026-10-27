package util;

public final class ClientConfig {

    public static final int SERVER_PORT = 8080;
    public static final String CONTEXT_PATH = "/ski-servlet";

    public static final int WARMUP_THREADS = 32;
    public static final int WARMUP_POSTS = 1000;
    public static final int TOTAL_POSTS = 500000;
    public static final int DEFAULT_MAIN_THREADS = 300;

    public final String HTTP_BASE_URL;
    public final String WS_BASE_URL;
    public final int MAIN_THREADS;
    public final int MAIN_POSTS;

    private ClientConfig(String serverIp, int mainThreads) {
        HTTP_BASE_URL = "http://" + serverIp + ":" + SERVER_PORT + CONTEXT_PATH;
        WS_BASE_URL = "ws://" + serverIp + ":" + SERVER_PORT + CONTEXT_PATH + "/leaderboard/";
        MAIN_THREADS = mainThreads;
        MAIN_POSTS = (TOTAL_POSTS - WARMUP_THREADS * WARMUP_POSTS) / MAIN_THREADS;
    }

    public static ClientConfig fromArgs(String[] args) {
        if (args.length < 1 || args.length > 2 || args[0].trim().isEmpty()) {
            throw new IllegalArgumentException("Usage: IngestPath <server_ip> [main_threads]");
        }
        String serverIp = args[0].trim();
        int mainThreads = DEFAULT_MAIN_THREADS;
        if (args.length == 2) {
            try {
                mainThreads = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("main_threads must be an integer.");
            }
        }
        int remaining = TOTAL_POSTS - WARMUP_THREADS * WARMUP_POSTS;
        if (mainThreads <= 0 || remaining % mainThreads != 0) {
            throw new IllegalArgumentException("main_threads must be positive and divide " + remaining + " evenly.");
        }
        return new ClientConfig(serverIp, mainThreads);
    }
}
