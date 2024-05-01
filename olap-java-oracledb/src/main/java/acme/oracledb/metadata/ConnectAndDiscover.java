package acme.oracledb.metadata;

import acme.oracledb.BaseExample;

public class ConnectAndDiscover extends BaseExample {
    @Override
    protected void run() throws Exception {
        //TODO: implement
    }

    public void execute(String[] args) {
        createContext = false;
        super.execute(convertToProperties(args));
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new ConnectAndDiscover().execute(args);
    }
}
