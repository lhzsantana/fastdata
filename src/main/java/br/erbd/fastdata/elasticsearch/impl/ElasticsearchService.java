package br.erbd.fastdata.elasticsearch.impl;

import br.erbd.fastdata.model.Twitter;
import br.erbd.fastdata.elasticsearch.AnalyticsService;
import org.springframework.stereotype.Component;

@Component
public class ElasticsearchService implements AnalyticsService {

    @Override
    public void index(Twitter twitter) {
        System.out.println(twitter);
    }
}
