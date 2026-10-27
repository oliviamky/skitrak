import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import model.LiftRide;
import model.ResponseMsg;
import util.RequestStats;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.logging.Logger;

public class SkierServlet extends HttpServlet {
    private static final int RESORT_ID_INDEX = 1;
    private static final int SEASON_ID_INDEX = 3;
    private static final int DAY_ID_INDEX = 5;
    private static final int SKIER_ID_INDEX = 7;
    private static final int HEALTH_PATH_LENGTH = 2;
    private static final int GET_URL_INDEX = 1;
    private static final int POST_PATH_LENGTH = 8;
    private static final int LIFT_RIDE_MAX_TIME = 360;
    private static final int LIFT_RIDE_ID_MAX = 40;
    private static final String GET_URL = "health";
    private static final String SEASON_PATH = "seasons";
    private static final String DAYS_PATH = "days";
    private static final String SKIERS_PATH = "skiers";
    private static final Logger logger = Logger.getLogger(SkierServlet.class.getName());
    private final Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws IOException {
        logger.info("GET request received: " + req.getServletPath());
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");
        String urlPath = req.getServletPath();
        RequestStats.incrementGet();
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write(gson.toJson(new ResponseMsg("missing parameters")));
            return;
        }
        String[] urlParts = urlPath.split("/");
        if (!isGetUrlValid(urlParts)) {
            logger.warning("Invalid URL: " + req.getServletPath());
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write(gson.toJson(new ResponseMsg("invalid url")));
        } else {
            logger.info("Valid GET request processed for: " + req.getServletPath());
            res.setStatus(HttpServletResponse.SC_OK);
            ResponseMsg response = new ResponseMsg("GET request processed for health");
            res.getWriter().write(gson.toJson(response));
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");
        String urlPath = req.getPathInfo();
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("{\"message\":\"missing parameters\"}");
            return;
        }
        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write("{\"message\":\"invalid url\"}");
            return;
        }
        // Parse JSON body
        try {
            LiftRide liftRide = gson.fromJson(req.getReader(), LiftRide.class);
            if (liftRide == null || !isLiftRideValid(liftRide)) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                res.getWriter().write("{\"message\":\"invalid JSON body\"}");
                return;
            }
            // Process the lift ride (in real app, save to database)
            res.setStatus(HttpServletResponse.SC_CREATED); //sc_created is 201
            res.getWriter().write("{\"message\":\"accepted\"}");
        } catch (JsonSyntaxException e) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write("{\"message\":\"invalid JSON format\"}");
        }
    }
    private boolean isGetUrlValid(String[] urlParts){
        //expected:["","health"];
        if(urlParts.length != HEALTH_PATH_LENGTH){
            return false;
        }
        return urlParts[GET_URL_INDEX].equals(GET_URL);
    }

    private boolean isUrlValid(String[] urlParts) {
        // Expected URL:
        //skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        // urlParts = ["", "1", "seasons", "2019", "days", "1", "skiers","123"]
        if (urlParts.length != POST_PATH_LENGTH) {
            return false;
        }
        try {
            // Check structure
            if (!urlParts[SEASON_ID_INDEX - 1].equals(SEASON_PATH)
                    || !urlParts[DAY_ID_INDEX - 1].equals(DAYS_PATH) ||
                    !urlParts[SKIER_ID_INDEX - 1].equals(SKIERS_PATH)) {
                return false;
            }
            // Validate numeric parameters
            int resortId = Integer.parseInt(urlParts[RESORT_ID_INDEX]); // resortID
            int seasonId = Integer.parseInt(urlParts[SEASON_ID_INDEX]); // seasonID
            int dayId = Integer.parseInt(urlParts[DAY_ID_INDEX]); // dayID
            int skierId = Integer.parseInt(urlParts[SKIER_ID_INDEX]); // skierID
            if(resortId <= 0 || seasonId <= 0 || dayId <= 0 || skierId <= 0){
                logger.info("URL path must be positive integers");
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isLiftRideValid(LiftRide liftRide) {
        return liftRide.getTime() > 0 && liftRide.getTime() <= LIFT_RIDE_MAX_TIME &&
                liftRide.getLiftID() > 0 && liftRide.getLiftID() <= LIFT_RIDE_ID_MAX;
    }
}