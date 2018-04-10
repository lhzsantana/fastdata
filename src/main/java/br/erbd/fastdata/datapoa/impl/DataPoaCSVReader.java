package br.erbd.fastdata.datapoa.impl;

import br.erbd.fastdata.elasticsearch.impl.ElasticsearchService;
import br.erbd.fastdata.model.Crash;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Component
public class DataPoaCSVReader implements br.erbd.fastdata.datapoa.DataPoaReader
{
    private String csvFile = "/home/luiz/IdeaProjects/fastdata/src/main/resources/acidentes2016.csv";
    private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public List<Crash> getCrashes() throws IOException {

        String line = "";
        String cvsSplitBy = ";";

        List<Crash> crashes = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            br.readLine();
            while ((line = br.readLine()) != null) {

                try {
                    String[] crashRaw = line.split(cvsSplitBy);

                    Crash crash = new Crash();

                    crash.setId(crashRaw[0]);
                    crash.setLat(Double.valueOf(crashRaw[2]));
                    crash.setLon(Double.valueOf(crashRaw[1]));
                    crash.setTipo(crashRaw[7]);
                    crash.setData(format.parse(crashRaw[10]));


                    crashes.add(crash);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return crashes;
    }

    public static void main(String [] args) throws IOException {
        DataPoaCSVReader reader = new DataPoaCSVReader();

        ElasticsearchService elasticsearchService = new ElasticsearchService();

        for(Crash crash : reader.getCrashes()){
            elasticsearchService.index(crash);
        }
    }
}
