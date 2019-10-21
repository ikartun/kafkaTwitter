package by.epam.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Kafka Producer that gets tweets on certain keywords from certain location
 * from twitter datasource and publishes to a kafka topic distributed by UserName
 *
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <topic-name> <keyword_1> ... <keyword_n>
 * <comsumerKey>	    - Twitter consumer key
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>	    - Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * <topic-name>		    - The kafka topic to subscribe to
 * <keyword_1>		    - The keyword to filter tweets
 * <keyword_n>		    - Any number of keywords to filter tweets
 *
 * @author Igor Kartun
 */

public class KafkaTwitterProducer {
    private final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
    private Producer<String, String> producer = initProducer(); // Kafka producer
    private TwitterStream twitterStream;

    private static final Logger logger = LoggerFactory.getLogger(KafkaTwitterProducer.class);

    public KafkaTwitterProducer(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        initTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret);
    }
    // Kafka producer config settings
    private static final String METADATA_BROKER_LIST = "localhost:9092";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ACKS = "all";
    private static final int RETRIES = 0;
    private static final int BATCH_SIZE = 16384;
    private static final int LINGER_MS = 1;
    private static final int BUFFER_MEMORY = 33554432;
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    // Location settings
    static final double[] EXACTGEO = {53.89, 27.57}; // Minsk coordinates
    static final String COUNTRY = "BELARUS";

    /**
     * Initializes Kafka producer using needed properties during creating main object.
     * @return Kafka producer
     */
    protected final Producer<String, String> initProducer() {
        // Add Kafka producer config settings
        Properties props = new Properties();
        props.put("metadata.broker.list", METADATA_BROKER_LIST);
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", ACKS);
        props.put("retries", RETRIES);
        props.put("batch.size", BATCH_SIZE);
        props.put("linger.ms", LINGER_MS);
        props.put("buffer.memory", BUFFER_MEMORY);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        return new KafkaProducer<>(props);
    }

    /**
     * Initializes TwitterStream with oAuth tokens during creating main object.
     * @param consumerKey       - Twitter consumer key.
     * @param consumerSecret    - Twitter consumer secret.
     * @param accessToken	    - Twitter access token.
     * @param accessTokenSecret	- Twitter access token secret.
     * @return TwitterStream object
     */
    protected final TwitterStream initTwitterStream(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        // Set twitter oAuth tokens in the configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).
                setOAuthConsumerKey(consumerKey).
                setOAuthConsumerSecret(consumerSecret).
                setOAuthAccessToken(accessToken).
                setOAuthAccessTokenSecret(accessTokenSecret);
        // Create twitterstream using the configuration
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        return twitterStream;
    }

    /**
     * Adds status listener to twitter stream
     * Gets twits from certain location.
     * Puts Twits to a queue
     */
    protected void addTwitterListener() {
        /** Twitter listener **/
        StatusListener statusListener = new StatusListener() {
            // The onStatus method is executed every time a new tweet comes in
            // Uses filtering by location
            @Override
            public void onStatus(Status status) {
                // at first try to filter by exact GPS position (a specific latitude/longitude "Point" coordinate) if user enabled it
                try {
                    double latitude = status.getGeoLocation().getLatitude();
                    double longitude = status.getGeoLocation().getLongitude();
                    if (isMatchedExactGeo(latitude, longitude)) {
                        queue.offer(status);
                    }
                }
                catch (NullPointerException e) {
                    // if exact geo position wasn't enabled, try to filter by Place
                    // Tweets with a Twitter "Place" contain a polygon, consisting of 4 lon-lat coordinates that define the general area (the "Place") from which the user is posting the Tweet
                    // Place will have correspondence country which is used as value to filter by
                    try {
                        if (status.getPlace().getCountry().toUpperCase().contains(COUNTRY)) {
                            queue.offer(status);
                        }
                    }
                    catch (NullPointerException e1) {
                        // if neither GPS position nor Place were set (consider these Tweets as not "geo-tagged" Tweets), try to filter by location set in User profile
                        try {
                            if (status.getUser().getLocation().toUpperCase().contains(COUNTRY)) {
                                queue.offer(status);
                            }
                        }
                        catch (NullPointerException e2) {
                            logger.info("No twits matched to your location.");
                        }
                    }
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                System.out.println("Got track limitation notice:" + i);
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                logger.info("Got scrub_geo event userId:" + l + "upToStatusId:" + l1);
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                logger.info("Got stall warning:" + stallWarning);
            }

            @Override
            public void onException(Exception e) {
                logger.info(e.getMessage());
            }
        };
        /** Bind the listener **/
        twitterStream.addListener(statusListener);
    }

    /**
     * Filter Tweets by keywords.
     * @param keyWords separated by spaces keywords and all is considered as a whole phrase.
     */
    public void filterByKeywords(String keyWords) {
        FilterQuery query = new FilterQuery();
        query.track(keyWords);
        twitterStream.filter(query);
    }

    /**
     * Checks if a Tweet with specified exact GPS location (latitude/longitude) matches with your City location.
     * @param latitude geographical latitude of exact Twit's location.
     * @param longitude geographical longitude of exact Twit's location.
     * @return true/false (whether exact Twit's location matches our location/or not)
     */
    boolean isMatchedExactGeo(double latitude, double longitude) {
        // since Twitter doesn't use precise coordinates - we would assume to match by almost the same coordinates
        // we assume our precision is about 0.5 and also applied rounding
        if (Math.round(latitude) != Math.round(EXACTGEO[0])
                && Math.round(latitude + 0.5) != Math.round(EXACTGEO[0])
                && Math.round(latitude - 0.5) != Math.round(EXACTGEO[0]))
            return false;
        else
            return (Math.round(longitude) == Math.round(EXACTGEO[1])
                    || Math.round(longitude + 0.5) == Math.round(EXACTGEO[1])
                    || Math.round(longitude - 0.5) == Math.round(EXACTGEO[1]));
    }

    /**
     * Checks if a Tweet with specified exact GPS location (latitude/longitude) matches with your City location.
     * @param topicName Kafka topic to send message to it.
     * @param userName Twit's user, used as a key to distribute across partitions by it.
     * @param text the full text of Twit with hashtag as well.
     * @return response from Kafka which contains detailed information about how message was stored in Kafka: partition, offset etc
     */
    Future<RecordMetadata> sendToKafka(String topicName, String userName, String text) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, userName, text);
        return producer.send(record);
    }

    /**
     * Polls for new tweets in the queue. If new tweets were added, send them to the Kafka topic.
     * Messages are stored to Kafka distributed by userName because userName is used as the key in ProducerRecord and Kafka chooses a partition based on a hash algorithm of the userName
     * @param topicName Kafka topic where to send messages.
     */
    public void loadTwitsToKafka(String topicName) {
        String userName;
        while (true) {
            Status ret = queue.poll();

            if (ret == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Something wrong waiting for a new Twit!");
                }
            } else {
                for (HashtagEntity hashtage : ret.getHashtagEntities()) {
                    userName = ret.getUser().getName();
                    Future<RecordMetadata> response = sendToKafka(topicName, userName, hashtage.getText());
                    try {
                        logger.info("The message " + hashtage + " went to partition " + response.get().partition() + ", offset " + response.get().offset());
                    } catch (InterruptedException e) {
                        logger.error("Something wrong, interrupted exception sending message: " + e.getMessage());
                    } catch (ExecutionException e) {
                        logger.error("Something wrong, execution exception sending message: " + e.getMessage());
                    }
                }
            }
        }
        // producer.close();
        // Thread.sleep(500);
        // twitterStream.shutdown();
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            logger.error(
                    "Usage: KafkaTwitterProducer <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token> <twitter-access-token-secret>");
            return;
        }

        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];
        String topicName = args[4];
        String keyWords = "";
        for (int i = 5; i < args.length; i++)
            keyWords = keyWords + " " + args[i];
        keyWords = keyWords.trim();

        KafkaTwitterProducer kafkaTwitterProducer = new KafkaTwitterProducer(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        kafkaTwitterProducer.addTwitterListener();
        kafkaTwitterProducer.filterByKeywords(keyWords);
        kafkaTwitterProducer.loadTwitsToKafka(topicName);
    }
}