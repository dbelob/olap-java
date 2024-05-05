package acme.oracledb.metadata;

import acme.oracledb.BaseExample;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.metadata.mdm.MdmMetadataProvider;
import oracle.olapi.session.UserSession;
import oracle.olapi.transaction.TransactionProvider;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class ConnectAndDiscover extends BaseExample {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConnectAndDiscover.class);

    private boolean isGlobalAwjOnly;

    @Override
    protected void run() throws Exception {
        log.info("Getting a JDBC OracleConnection.");
        OracleConnection conn = null;

        try {
            // Create an OracleConnection using the command-line arguments that
            // the BaseExample11g.convertToProperties method stored in a
            // Properties object.
            OracleDataSource ods = new OracleDataSource();

            ods.setURL(props.getProperty("url"));
            ods.setUser(props.getProperty("user"));
            ods.setPassword(props.getProperty("password"));

            conn = (oracle.jdbc.OracleConnection) ods.getConnection();
        } catch (SQLException e) {
            log.error("Connection attempt failed. ", e);
        }

        log.info("Creating a DataProvider.");
        DataProvider dp = new DataProvider();

        try {
            UserSession session = dp.createSession(conn);
        } catch (SQLException e) {
            log.error("Could not create a UserSession. ", e);
        }

        log.info("Getting the TransactionProvider.");
        TransactionProvider tp = dp.getTransactionProvider();

        // Run the discovering metadata examples before closing the data provider
        // and the connection.

        log.info("Creating an MdmMetadataProvider.");
        MdmMetadataProvider mp = null;

        try {
            mp = (MdmMetadataProvider) dp.getMdmMetadataProvider();
        } catch (Exception e) {
            log.error("Cannot get the MDM metadata provider. ", e);
        }

        // Get only metadata objects for the GLOBAL schema.
        // To get all the available metadata objects, specify false.
        setGlobalAwjOnly(true);

        if (!isGlobalAwjOnly) {
            log.info("Getting the MdmSchema Objects.");
        }
//        MdmDatabaseSchema mdmGlobalSchema = getSchemas(mp);

        //TODO: implement
    }

    public void execute(String[] args) {
        createContext = false;
        super.execute(convertToProperties(args));
    }

    public void setGlobalAwjOnly(boolean isGlobalAwjOnly) {
        this.isGlobalAwjOnly = isGlobalAwjOnly;
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new ConnectAndDiscover().execute(args);
    }
}
