package acme.oracledb.source;

import acme.oracledb.BaseExample;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.data.source.Source;
import oracle.olapi.metadata.mdm.MdmDatabaseSchema;
import oracle.olapi.metadata.mdm.MdmMetadataProvider;
import oracle.olapi.metadata.mdm.MdmRootSchema;
import oracle.olapi.metadata.mdm.MdmStandardDimension;
import org.slf4j.LoggerFactory;

public class UnderstandingSourceObjects extends BaseExample {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(UnderstandingSourceObjects.class);

    private DataProvider dp;
    private MdmDatabaseSchema mdmDBSchema;

    @Override
    protected void run() throws Exception {
        dp = getDataProvider();
        MdmMetadataProvider mp = getMdmMetadataProvider();
        MdmRootSchema mdmRootSchema = (MdmRootSchema) mp.getRootSchema();

        // Get the name of the user for the current session.
        String userName = getContext().getUser();

        // Get the MdmDatabaseSchema for the user.
        mdmDBSchema = mdmRootSchema.getDatabaseSchema(userName);

        // Informal example.
        gettingTheMembersOfADimension();

        //TODO: implement
    }

    public void gettingTheMembersOfADimension() {
        // Informal example in the Elements and Values of a Source topic of Oracle OLAP Java API Developer's Guide.

        MdmStandardDimension mdmChanDim = mdmDBSchema.findOrCreateStandardDimension("CHANNEL");
        Source chanDim = mdmChanDim.getSource();

        // We do not need to commit the Transaction because we did not create a derived Source.
        log.info("Informal example that gets the values of the elements of the Source for the {} dimension.", mdmChanDim.getName());
        getContext().displayResult(chanDim);
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new UnderstandingSourceObjects().execute(args);
    }
}
