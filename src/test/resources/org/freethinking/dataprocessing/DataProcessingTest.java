package org.freethinking.dataprocessing;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.api.services.youtube.model.Video;

public class DataProcessingTest {

    private static String url1 = "https://www.youtube.com/watch?v=nfUele5xUhg";
    private static String url2 = "https://www.abcd.com/watch?v=nfUele5xUhg";

    @Test
    public void getVideoIdFromYouTubeUrlTest() {

        String url = "https://www.youtube.com/watch?v=nfUele5xUhg";

        DataProcessing dataProcessing = new DataProcessing();
        String id = dataProcessing.getVideoIdFromYoutubeUrl(url);
        Assert.assertEquals("nfUele5xUhg", id);
    }

    @Test
    public void youTubeUrlTest() {

        DataProcessing dataProcessing = new DataProcessing();
        Boolean linkStatus = dataProcessing.isYouTubeLink(url1);
        Assert.assertEquals(true, linkStatus);
    }

    @Test
    public void notYouTubeUrlTest() {

        DataProcessing dataProcessing = new DataProcessing();
        Boolean linkStatus = dataProcessing.isYouTubeLink(url2);
        Assert.assertEquals(false, linkStatus);
    }

    @Test
    public void getYouTubeUrlsAndStatsTest() throws MalformedURLException, IOException, InterruptedException {

        ArrayList<String> urlList = new ArrayList<String>();
        urlList.add(url1);
        urlList.add(url2);

        DataProcessing dataProcessing = new DataProcessing();
        List<Video> videoList = dataProcessing.getYouTubeUrlsAndStats(urlList);
        Assert.assertEquals(1, videoList.size());

    }
}
