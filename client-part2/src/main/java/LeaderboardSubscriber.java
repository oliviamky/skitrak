import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ClientEndpoint
public class LeaderboardSubscriber {
//    private static final String DEFAULT_URI = util.ClientConfig.WS_BASE_URL;
    private static final int DELAY_DEFAULT = 1000;
    private static final int DELAY_MULTIPLIER = 2;
    private static final int DELAY_MAX = 10000;
    private WebSocketContainer container;
    private URI uri;
    private volatile Session currentSession;
    private final Object sessionLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean reconnect = new AtomicBoolean(false);
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final AtomicInteger connCount = new AtomicInteger(0);
    private final AtomicInteger reconnCount = new AtomicInteger(0);
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private volatile long time;

    @OnOpen
    public void onOpen(Session session){
        connCount.incrementAndGet();
    }

    @OnMessage
    public void onMessage(String message) {
//        System.out.println(message);
        long receiveTime = System.currentTimeMillis();
        messageCount.incrementAndGet();
        JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        String timestamp = json.get("serverTimestamp").getAsString();
        long serverTime = Instant.parse(timestamp).toEpochMilli();
        long latency = receiveTime - serverTime;
        latencies.add(Long.valueOf(latency));
    }

    @OnClose
    public void onClose(Session session, CloseReason reason){
        int code = reason.getCloseCode().getCode();
        boolean shouldRetry;
        synchronized (sessionLock){
            shouldRetry = running.get() && currentSession == session;
            if(currentSession == session){
                currentSession = null;
            }
        }
        if(shouldRetry){
            System.out.println("Unexpected close. Need to reconnect");
            retry();
        }
    }

    @OnError
    public void onError(Session session, Throwable error){
        System.err.println("WebSocket error " + error.getMessage());
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    private void retry(){
        if(!running.get() || !reconnect.compareAndSet(false,true)){
            return;
        }
        Thread thread = new Thread(() -> {
            try{
                int delay = DELAY_DEFAULT;
                while(running.get()){
                    try{
                        System.out.println("Reconnecting. ");
                        Session newSession = container.connectToServer(this, uri);
                        boolean shouldClose;
                        synchronized (sessionLock) {
                            shouldClose = !running.get();
                            if(!shouldClose) {
                                currentSession = newSession;
                            }
                        }
                        if (shouldClose) {
                            newSession.close();
                            break;
                        }
                        if(!newSession.isOpen()){
                            System.err.println("Reconnected session is already closed.");
                            Thread.sleep(delay);
                            delay = Math.min(delay * DELAY_MULTIPLIER, DELAY_MAX);
                            continue;
                        }
                        reconnCount.incrementAndGet();
                        System.out.println("Reconnected.");
                        break;
                    } catch (Exception e) {
                        System.err.println("Reconnection failed " + e.getMessage());
                        Thread.sleep(delay);
                        delay = Math.min(delay * DELAY_MULTIPLIER, DELAY_MAX);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally{
                reconnect.set(false);
                if(running.get()){
                    Session session = currentSession;
                    if(session == null || !session.isOpen()){
                        retry();
                    }
                }
            }
        });
        thread.start();
    }

    public void connect(String url, int resortId) throws Exception{
        container = ContainerProvider.getWebSocketContainer();
        uri = URI.create(url + resortId);
        long start = System.currentTimeMillis();
        Session newSession = container.connectToServer(this, uri);
        time = System.currentTimeMillis() - start;
        boolean shouldClose;
        synchronized (sessionLock){
            shouldClose = !running.get();
            if(!shouldClose){
                currentSession = newSession;
            }
        }
        if(shouldClose){
            newSession.close();
        } else if(!newSession.isOpen()){
            retry();
        }
    }

    public void disconnect() throws IOException{
        Session session;
        synchronized (sessionLock) {
            running.set(false);
            session = currentSession;
            currentSession = null;
        }
        if (session != null && session.isOpen()) {
            session.close();
        }
    }
    public int getConnectionCount() {
        return connCount.get();
    }

    public int getReconnectionCount() {
        return reconnCount.get();
    }

    public long getTime(){
        return time;
    }

    public List<Long> getLatencies(){
        synchronized (latencies){
            return new ArrayList<>(latencies);
        }
    }

    public static void main(String[] args) throws Exception {
    }
}