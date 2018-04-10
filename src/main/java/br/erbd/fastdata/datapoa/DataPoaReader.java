package br.erbd.fastdata.datapoa;

import br.erbd.fastdata.model.Crash;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public interface DataPoaReader {

    public List<Crash> getCrashes() throws IOException;

}