package by.epam.bigdata;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;
import twitter4j.TwitterStream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaTwitterProducerTest {
    private static final String CONSUMERKEY = "djStkqnUASWneyaVNgrQ3R1zZ";
    private static final String CONSUMERSECRET = "ZVQAR42WPjayec5JVOVWfVojHrsGbXrLSVwARNjDM0YnC6vHjy";
    private static final String ACCESSTOKEN = "1166345446509428736-WNKSRcDmkH8NximSaEpMsHjDTxzjJa";
    private static final String ACCESSTOKENSECRET = "MUmojl7AihmK9Vb5D73cMLCwdjDRiIWL6QP4nCTTE9PUq";
    private KafkaTwitterProducer kafkaTwitterProducer = new KafkaTwitterProducer(CONSUMERKEY, CONSUMERSECRET, ACCESSTOKEN, ACCESSTOKENSECRET);

    /**
     * If a Twit's location almost matches my location - expected result is true, otherwise - false.
     */
    @Test
    public void testIsMatchedExactGeo() {
        double[] myLocation = KafkaTwitterProducer.EXACTGEO;

        double twitLocationLat = 53.55;
        double twitLocationLon = 27.50;
        boolean realRes = kafkaTwitterProducer.isMatchedExactGeo(twitLocationLat, twitLocationLon);
        Assert.assertEquals(true, realRes);

        twitLocationLat = 54.41;
        twitLocationLon = 27.99;
        realRes = kafkaTwitterProducer.isMatchedExactGeo(twitLocationLat, twitLocationLon);
        Assert.assertEquals(true, realRes);

        twitLocationLat = 54.49;
        twitLocationLon = 29.02;
        realRes = kafkaTwitterProducer.isMatchedExactGeo(twitLocationLat, twitLocationLon);
        Assert.assertEquals(false, realRes);

        twitLocationLat = 54.90;
        twitLocationLon = 28.00;
        realRes = kafkaTwitterProducer.isMatchedExactGeo(twitLocationLat, twitLocationLon);
        Assert.assertEquals(true, realRes);

        twitLocationLat = 55.11;
        twitLocationLon = 27.88;
        realRes = kafkaTwitterProducer.isMatchedExactGeo(twitLocationLat, twitLocationLon);
        Assert.assertEquals(false, realRes);
    }

    /**
     * Checking whether messages are sent to Kafka and stored properly
     */
    @Test
    public void testSendToKafka() throws ExecutionException, InterruptedException {
        String topicName = "topic1";

        String userName = "User1";
        String text = "I am Studying big data ai machine learning course #BigData";

        Future<RecordMetadata> metadataFuture = kafkaTwitterProducer.sendToKafka(topicName, userName, text);
        long realRes = metadataFuture.get().offset();
        Assert.assertEquals(true, realRes > 0);

        userName = "User2";
        text = "";

        long previousOffset = realRes;
        metadataFuture = kafkaTwitterProducer.sendToKafka(topicName, userName, text);
        realRes = metadataFuture.get().offset();
        Assert.assertEquals(true, realRes > previousOffset);
    }

    /**
     * Checking whether Oauth tokens were set successfully for configuration of twitter stream
     */
    @Test
    public void testInitTwitterStream() {
        String consumerKey = CONSUMERKEY;
        String consumerSecret = CONSUMERSECRET;
        String accessToken = ACCESSTOKEN;
        String accessTokenSecret = ACCESSTOKENSECRET;
        TwitterStream twitterStream = new KafkaTwitterProducer(consumerKey, consumerSecret, accessToken, accessTokenSecret).initTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        String realRes = twitterStream.getConfiguration().getOAuthConsumerKey() + twitterStream.getConfiguration().getOAuthConsumerSecret() +  twitterStream.getConfiguration().getOAuthAccessToken() +  twitterStream.getConfiguration().getOAuthAccessTokenSecret();
        Assert.assertEquals(consumerKey + consumerSecret + accessToken + accessTokenSecret, realRes);

        consumerKey = null;
        consumerSecret = null;
        accessToken = null;
        accessTokenSecret = null;
        twitterStream = new KafkaTwitterProducer(consumerKey, consumerSecret, accessToken, accessTokenSecret).initTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        realRes = twitterStream.getConfiguration().getOAuthConsumerKey() + twitterStream.getConfiguration().getOAuthConsumerSecret() +  twitterStream.getConfiguration().getOAuthAccessToken() +  twitterStream.getConfiguration().getOAuthAccessTokenSecret();
        System.out.println("realRes = " + realRes);
        System.out.println("expRes = " + consumerKey + consumerSecret + accessToken + accessTokenSecret);
        Assert.assertEquals(consumerKey + consumerSecret + accessToken + accessTokenSecret, realRes);

        consumerKey = "";
        consumerSecret = "";
        accessToken = "";
        accessTokenSecret = "";
        twitterStream = new KafkaTwitterProducer(consumerKey, consumerSecret, accessToken, accessTokenSecret).initTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        realRes = twitterStream.getConfiguration().getOAuthConsumerKey() + twitterStream.getConfiguration().getOAuthConsumerSecret() +  twitterStream.getConfiguration().getOAuthAccessToken() +  twitterStream.getConfiguration().getOAuthAccessTokenSecret();
        System.out.println("realRes = " + realRes);
        System.out.println("expRes = " + consumerKey + consumerSecret + accessToken + accessTokenSecret);
        Assert.assertEquals(consumerKey + consumerSecret + accessToken + accessTokenSecret, realRes);
    }
}
