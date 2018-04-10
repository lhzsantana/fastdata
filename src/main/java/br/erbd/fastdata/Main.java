package br.erbd.fastdata;

import br.erbd.fastdata.datapoa.DataPoaReader;
import br.erbd.fastdata.model.Twitter;
import br.erbd.fastdata.elasticsearch.AnalyticsService;
import br.erbd.fastdata.spark.TwitterStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan
@Component
public class Main implements CommandLineRunner {

    @Autowired
    DataPoaReader dataPoaReader;

    @Autowired
    private TwitterStream twitterStream;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        twitterStream.readStream();
    }

}