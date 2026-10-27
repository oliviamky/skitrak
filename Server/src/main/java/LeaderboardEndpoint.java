import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServerEndpoint("/leaderboard/{resortID}")
public class LeaderboardEndpoint {
    // resortID -> sessions subscribed to that resort
    private static final int HARD_CODED_RANK_FIRST = 1;
    private static final int HARD_CODED_RANK_SECOND = 2;
    private static final int HARD_CODED_SKIER_FIRST = 40213;
    private static final int HARD_CODED_SKIER_SECOND = 11872;
    private static final int HARD_CODED_VERTICAL_FIRST = 44120;
    private static final int HARD_CODED_VERTICAL_SECOND = 43880;
    private static final long PUSH_INTERVAL_MS = 500;
    private static final Gson GSON = new Gson();
    private static final Map<String, Set<Session>> subscribers = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService pusher = Executors.newSingleThreadScheduledExecutor();
    private static final Logger logger = Logger.getLogger(LeaderboardEndpoint.class.getName());
    static {
        // Dummy: every 500ms, push a synthetic top-N to each resort's
        pusher.scheduleAtFixedRate(LeaderboardEndpoint::pushAll, PUSH_INTERVAL_MS, PUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    @OnOpen
    public void onOpen(Session session, @PathParam("resortID") String resortId){
        subscribers.computeIfAbsent(resortId, k -> ConcurrentHashMap.newKeySet()).add(session);
    }

    @OnClose
    public void onClose(Session session, @PathParam("resortID") String resortId){
        subscribers.computeIfPresent(resortId, (key, sessions) -> {
            sessions.remove(session);
            return sessions.isEmpty() ? null : sessions;
        });
    }

    @OnError
    public void onError(Session session, Throwable t) {
        // log, then clean up the session
        logger.log(Level.SEVERE, "WebSocket Error", t);
        String resortId = session.getPathParameters().get("resortID");
        if(resortId != null) {
            Optional.ofNullable(subscribers.get(resortId)).
                    ifPresent(s -> {
                        s.remove(session);
                    });
        }
    }

    private static void pushAll() {
        for (Map.Entry<String, Set<Session>> e : subscribers.entrySet()) {
            String json = syntheticLeaderboard(e.getKey()); //
            //canned/random top-N + serverTimestamp
            for (Session s : e.getValue()) {
                try {
                    if (s.isOpen()) {
                        s.getAsyncRemote().sendText(json);
                    }
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "Failed to push leaderboard", ex);
                }
            }
        }
    }

    //Canned contents
    private static String syntheticLeaderboard(String key){
        JsonObject leaderboard = new JsonObject();
        leaderboard.addProperty("resortID", key);
        leaderboard.addProperty("serverTimestamp", Instant.ofEpochMilli(System.currentTimeMillis()).toString());
        JsonArray top = new JsonArray();
        int[][] data = new int[][]{{HARD_CODED_RANK_FIRST, HARD_CODED_SKIER_FIRST, HARD_CODED_VERTICAL_FIRST},
                {HARD_CODED_RANK_SECOND, HARD_CODED_SKIER_SECOND, HARD_CODED_VERTICAL_SECOND}};
        for(int i = 0; i < data.length; i++){
            JsonObject inside = new JsonObject();
            inside.addProperty("rank", data[i][0]);
            inside.addProperty("skierID", data[i][1]);
            inside.addProperty("vertical", data[i][2]);
            top.add(inside);
        }
        leaderboard.add("top", top);
        return GSON.toJson(leaderboard);
    }
}