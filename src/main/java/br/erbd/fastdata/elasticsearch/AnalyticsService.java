package br.erbd.fastdata.elasticsearch;

import br.erbd.fastdata.model.Twitter;

public interface AnalyticsService {

    public void index(Twitter twitter);
}
