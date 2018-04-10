package br.erbd.fastdata.spark.impl;

import br.erbd.fastdata.model.Twitter;
import br.erbd.fastdata.spark.TwitterStream;
import br.erbd.fastdata.voltdb.impl.DatabaseService;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

@Component
public class SparkTwitterStream implements TwitterStream {

    @Autowired
    DatabaseService databaseService;

    SparkConf sparkConf = new SparkConf()
            .setAppName("JavaTwitterHashTagJoinSentiments")
            .setMaster("spark://luiz-Inspiron-3542:7077");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    @Override
    public void readStream() throws InterruptedException {

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        TwitterUtils.createStream(jssc);

        String[] filters = { "#Car" };
        TwitterUtils.createStream(jssc, twitterAuth, filters).print();

        jssc.start();
        jssc.awaitTermination();

        Twitter twitter = new Twitter();

        databaseService.insert(twitter);
    }

    public static void main(String [] args) throws InterruptedException {
        SparkTwitterStream stream = new SparkTwitterStream();

        stream.readStream();
    }
}
