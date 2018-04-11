package br.erbd.fastdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class SparkTwitterStream {


    transient SparkConf sparkConf = new SparkConf()
            .setAppName("JavaTwitterHashTagJoinSentiments")
            .setMaster("spark://luiz-Inspiron-3542:7077");

    transient JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    public void readStream() throws InterruptedException {

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getText(); }
                }
        );

        twitterStream.print();

        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String [] args) throws InterruptedException {

        String consumerKey = "KHDNVf4vkurce5ZxUOxVyLScR";
        String consumerSecret = "gaI3gVVLB2aHV7NzuZDsDkeyexRh2KncS6XAZzMemTXAu1VWzE";
        String accessToken = "14642944-zRQmGmR2JTnxjMHwLzJOj5ipTlbSFTjttwjsaVE1p";
        String accessTokenSecret = "jeZQ4Biu1bIkyZWbgWi4hTDzVDzwIjezDV5RKTA3EcXAe";

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        SparkTwitterStream stream = new SparkTwitterStream();

        stream.readStream();
    }
}
