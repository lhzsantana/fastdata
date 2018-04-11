package br.erbd.fastdata.voltdb;

import br.erbd.fastdata.model.Tweet;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class VoltDBService{

    public void insert(Tweet tweet) {

        Client client = null;
        ClientConfig config = null;
        try {
            client = ClientFactory.createClient();
            client.createConnection("localhost");

            client.callProcedure("users.insert", tweet.getId(), tweet.getUser());
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) {
        VoltDBService reader = new VoltDBService();

        Tweet tweet = new Tweet();
        reader.insert(tweet);
    }
}
