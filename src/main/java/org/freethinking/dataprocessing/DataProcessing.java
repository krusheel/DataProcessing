package org.freethinking.dataprocessing;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.validator.UrlValidator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.youtube.model.Video;

public class DataProcessing {

    private static Logger logger = LoggerFactory.getLogger(DataProcessing.class);
    private static YouTubeVideoStatistics youTubeStats;
    private static int MAX_REDIRECT_COUNT = 15;  // Max number of redirects

    private JsonFactory jsonFactory = new JsonFactory();
    private ObjectMapper mapper = new ObjectMapper();

    public DataProcessing() {
        youTubeStats = new YouTubeVideoStatistics();
    }

    public JsonParser getFileParser(String fileName) throws IOException, InterruptedException {

        logger.info("Working Directory : {}", System.getProperty("user.dir"));

        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(new File(fileName));

        } catch (IOException ex) {
            logger.error("Json file  {} not available ", fileName);
        }

        return parser;
    }

    public List<Video> getYouTubeUrlAndStats(JsonParser parser) throws InterruptedException, IOException {

        List<Video> videoList = new ArrayList<Video>();
        int tCount = 0;
        int yUrlCount = 0;
        
        int soCount = 0;
        int objCount = 0;

        if (parser == null)
            return videoList;

        try {

            JsonToken token;
            token = parser.nextToken();
            if (token != JsonToken.START_ARRAY) {
                logger.error("File should start with [");
                return videoList;
            }

            while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                switch (token) {

                // Starts a new object, clear the map
                case START_OBJECT:
                    soCount += 1;
                    break;

                // For each field-value pair, store it in the map 'fields'
                case FIELD_NAME:
                    String field = parser.getCurrentName();
                    token = parser.nextToken();
                    if (field.equals("text") && (token == JsonToken.VALUE_STRING) ) {//
                        String value = parser.getText();
                        tCount++;

                        ArrayList<String> urls = pullLinks(value);
                        if(urls.size() != 0) {
                            logger.info(" Text Field Count : " + tCount + " Youtube Url Count : " + yUrlCount);
                        }
                        List<Video> currYouTubeStats = getYouTubeUrlsAndStats(urls);
                        videoList.addAll(currYouTubeStats);
                        yUrlCount += currYouTubeStats.size();
                        
                        if(currYouTubeStats.size() != 0) {                            
                            // ToDo : Only write new elements to DB and Sort only after completion
                            videoList = sortByLikes(videoList);
                            mapper.writeValue(new File("./sortedOutput.json"), videoList);
                            logger.info(" Text Field Count : " + tCount + " Youtube Url Count : " + yUrlCount);
                        }                        
                    }
                    else if (token == JsonToken.START_ARRAY) {
                        parser.skipChildren();
                    }
                    else if(token == JsonToken.START_OBJECT) {
                        parser.skipChildren();
                    }
                    else {
                    }                    
                    break;

                // Do something with the field-value pairs
                case END_OBJECT:
                    soCount -= 1;
                    objCount += 1;
                    if(objCount % 50 == 0)
                        logger.info(" #### Number of Objects read : " + objCount);
                    break;
                }

            }
        } catch (JsonParseException e) {
            logger.error("Error while parsing");
        } finally {
            parser.close();
        }

        return videoList;

    }

    public List<Video> sortByLikes(List<Video> videoList) {

        Collections.sort(videoList, new Comparator<Video>() {
            public int compare(Video v1, Video v2) {
                return v2.getStatistics().getLikeCount().compareTo(v1.getStatistics().getLikeCount());
            }
        });

        return videoList;
    }

    private void addToTweetsList(String value, List<String> tweets) {

        tweets.add(value);
        System.out.println(value);
    }

    // Pull all links from the body for easy retrieval
    private ArrayList pullLinks(String text) {
        ArrayList links = new ArrayList();

        String regex = "\\(?\\b(https?://|www[.])[-A-Za-z0-9+&amp;@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&amp;@#/%=~_()|]";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(text);
        while (m.find()) {
            String urlStr = m.group();
            if (urlStr.startsWith("(") && urlStr.endsWith(")"))
            {
                urlStr = urlStr.substring(1, urlStr.length() - 1);
            }
            links.add(urlStr);
        }
        return links;
    }

    public static String getLongUrl(String shortUrl) throws MalformedURLException, IOException {

        String result = shortUrl;
        String header;
        
        int redCount = 0; // # of redirects
        do {
            URL url = null;
            try {
                url = new URL(result);
            } catch (MalformedURLException ex) {
                logger.error("Malformed Exception : {}", result);
                result = "";
                break;
            }

            HttpURLConnection.setFollowRedirects(false);
            URLConnection conn = url.openConnection();
            header = conn.getHeaderField(null);
            if (header == null) {
                // logger.error("Header is null : {}", result);
                result = "";
                break;
            }
            String location = conn.getHeaderField("location");
            if (location != null) {
                result = location;
                // System.out.println("Redirect Url : " + result);
            }
            
            if(redCount == MAX_REDIRECT_COUNT) {
                result = "";
            }
            redCount += 1;
        } while (header.contains("301") && redCount <= MAX_REDIRECT_COUNT);

        return result;
    }

    public static Boolean isYouTubeLink(String link) {

        Boolean youTubeUrl = false;
        String pattern = "https?:\\/\\/(?:[0-9A-Za-z-]+\\.)?(?:youtu\\.be\\/|youtube\\.com\\S*[^\\w\\-\\s])([\\w\\-]{11})(?=[^\\w\\-]|$)(?![?=&+%\\w]*(?:['\"][^<>]*>|<\\/a>))[?=&+%\\w]*";
        if (!link.isEmpty() && link.matches(pattern)) {
            youTubeUrl = true;
        }

        return youTubeUrl;
    }

    public List<Video> getYouTubeUrlsAndStats(ArrayList<String> urls) throws MalformedURLException, IOException,
            InterruptedException {

        List<Video> videoList = new ArrayList<Video>();

        for (String shortUrl : urls) {

            UrlValidator urlValidator = new UrlValidator();
            urlValidator.isValid(shortUrl);
            String longUrl = getLongUrl(shortUrl);
            if (isYouTubeLink(longUrl)) {
                String videoId = getVideoIdFromYoutubeUrl(longUrl);
                videoList = youTubeStats.getVideoStatsFromId(videoId);
                System.out.println("Is You Yube Url : " + isYouTubeLink(longUrl) + "  Url : " + longUrl);
            }
            Thread.sleep(100);
        }
        return videoList;
    }

    public String getVideoIdFromYoutubeUrl(String url) {

        String id = "";

        Pattern compiledPattern = Pattern.compile("(?<=v=).*?(?=&|$)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = compiledPattern.matcher(url);
        if (matcher.find()) {
            id = matcher.group();
        }

        return id;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if(args.length < 1 ) {
            logger.info("usage java -jar DataProcessing-1.0.jar <inputfile>");
            System.exit(1);
        }
        //String fileName = "./sample_tweets_data.json";//"./test_sample.json";//
        String fileName = args[0];        

        DataProcessing dataProcessor = new DataProcessing();

        JsonParser parser = dataProcessor.getFileParser(fileName);
        List<Video> videoList = dataProcessor.getYouTubeUrlAndStats(parser);
        List<Video> sortedVideoList = dataProcessor.sortByLikes(videoList);
    }

}
