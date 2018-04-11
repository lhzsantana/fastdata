package br.erbd.fastdata.twitter4j;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import br.erbd.fastdata.elasticsearch.ElasticsearchService;
import br.erbd.fastdata.model.Tweet;
import br.erbd.fastdata.voltdb.VoltDBService;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4J {

    private static final String TWITTER_CONSUMER_KEY = "KHDNVf4vkurce5ZxUOxVyLScR";
    private static final String TWITTER_SECRET_KEY = "gaI3gVVLB2aHV7NzuZDsDkeyexRh2KncS6XAZzMemTXAu1VWzE";
    private static final String TWITTER_ACCESS_TOKEN = "14642944-zRQmGmR2JTnxjMHwLzJOj5ipTlbSFTjttwjsaVE1p";
    private static final String TWITTER_ACCESS_TOKEN_SECRET = "jeZQ4Biu1bIkyZWbgWi4hTDzVDzwIjezDV5RKTA3EcXAe";

    public static void main(String arg[]) throws UnknownHostException {

        ElasticsearchService elasticsearchService = new ElasticsearchService();
        VoltDBService voltDBService = new VoltDBService();

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
                .setOAuthConsumerSecret(TWITTER_SECRET_KEY)
                .setOAuthAccessToken(TWITTER_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(TWITTER_ACCESS_TOKEN_SECRET);
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        try {
            Query query = new Query();

            GeoLocation location = new GeoLocation(-30.0277, -51.2287 );
            String unit = Query.KILOMETERS.toString();
            query.setGeoCode(location, 20, unit);

            QueryResult result;

            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();

                for (Status status : tweets) {

                    Tweet tweet = new Tweet();

                    if(status.getGeoLocation()==null) {
                        tweet.setLon(generateDouble(-31d, -30d));
                        tweet.setLat(generateDouble(-51d, -50d));
                    }else{
                        tweet.setLon(status.getGeoLocation().getLongitude());
                        tweet.setLat(status.getGeoLocation().getLatitude());
                    }

                    tweet.setBody(status.getText());
                    tweet.setUser(status.getUser().getName());

                    try {
                        elasticsearchService.index(tweet);
                        voltDBService.insert(tweet);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText() + " - " + status.getGeoLocation());
                }

            } while ((query = result.nextQuery()) != null);

        } catch (TwitterException te) {
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }

    }

    private static Double generateDouble(Double rangeMin, Double rangeMax){
        Random r = new Random();
        return rangeMin + (rangeMax - rangeMin) * r.nextDouble();
    }

}