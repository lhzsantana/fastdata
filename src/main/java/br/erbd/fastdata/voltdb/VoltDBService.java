package br.erbd.fastdata.voltdb;

import br.erbd.fastdata.model.Twitter;
import br.erbd.fastdata.voltdb.impl.DatabaseService;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class VoltDBService implements DatabaseService{

    @Override
    public void insert(Twitter twitter) {

        Client client = null;
        ClientConfig config = null;
        try {
            client = ClientFactory.createClient();
            client.createConnection("localhost");

            VoltTable[] results;
            client.callProcedure("HELLOWORLD.insert","American","Howdy","Earth");
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) {
        VoltDBService reader = new VoltDBService();

        Twitter twitter = new Twitter();
        reader.insert(twitter);
    }
}
