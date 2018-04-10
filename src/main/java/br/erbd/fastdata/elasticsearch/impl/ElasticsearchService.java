package br.erbd.fastdata.elasticsearch.impl;

import br.erbd.fastdata.model.Crash;
import br.erbd.fastdata.model.Twitter;
import br.erbd.fastdata.elasticsearch.AnalyticsService;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Component
public class ElasticsearchService implements AnalyticsService {

    TransportClient client = null;

    public ElasticsearchService() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

    }

    @Override
    public void index(Twitter twitter) throws IOException {

        XContentBuilder content =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("location")
                        .latlon( twitter.getLat(), twitter.getLon())
                        .endObject();

        indexer("twitter", content, twitter.getId());
    }

    @Override
    public void index(Crash crash) throws IOException {

        XContentBuilder content =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("location")
                        .latlon( crash.getLat(), crash.getLon())
                        .field("tipo", crash.getTipo())
                        .field("data", crash.getData())
                        .endObject()
                ;

        System.out.println(content.string());

        indexer("crash", content, crash.getId());

    }

    private void indexer(String type, XContentBuilder content, String id){

        try {
            IndexResponse response = client.prepareIndex()
                    .setId(id)
                    .setIndex("erbd")
                    .setType(type)
                    .setSource(content)
                    .execute().actionGet();
            System.out.println(response.status());

        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
