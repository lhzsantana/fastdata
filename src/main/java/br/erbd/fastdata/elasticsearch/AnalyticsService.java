package br.erbd.fastdata.elasticsearch;

import br.erbd.fastdata.model.Crash;
import br.erbd.fastdata.model.Twitter;

import java.io.IOException;

public interface AnalyticsService {

    public void index(Twitter twitter) throws IOException;

    public void index(Crash crash) throws IOException;
}
