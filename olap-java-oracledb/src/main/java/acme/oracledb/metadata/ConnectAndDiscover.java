package acme.oracledb.metadata;

import acme.oracledb.BaseExample;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.metadata.deployment.AW;
import oracle.olapi.metadata.mdm.*;
import oracle.olapi.session.UserSession;
import oracle.olapi.transaction.TransactionProvider;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConnectAndDiscover extends BaseExample {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConnectAndDiscover.class);

    private boolean isGlobalAwOnly;
    private AW globalAw;

    @Override
    protected void run() throws Exception {
        log.info("Getting a JDBC OracleConnection.");
        OracleConnection conn = null;

        try {
            // Create an OracleConnection using the command-line arguments that
            // the BaseExample.convertToProperties method stored in a
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
        MdmMetadataProvider mp;

        try {
            mp = dp.getMdmMetadataProvider();

            // Get only metadata objects for the GLOBAL schema.
            // To get all the available metadata objects, specify false.
            setGlobalAwOnly(true);

            if (!isGlobalAwOnly) {
                log.info("Getting the MdmSchema Objects.");
            }

            MdmDatabaseSchema mdmGlobalSchema = getSchemaAndPrintAllSchemas(mp, "GLOBAL");

            // Get the GLOBAL analytic workspace.
            setGlobalAw(mdmGlobalSchema, "GLOBAL");

            if (!isGlobalAwOnly) {
                log.info("Getting the Dimensions and Measures of an MdmDatabaseSchema.");
            } else {
                log.info("Getting the Dimensions and Measures of an AW.");
            }

            getObjects(mdmGlobalSchema);

            log.info("Getting the Dimensions and Measures of an MdmCube.");
            getCubeObjects(mdmGlobalSchema);

            log.info("Getting the Hierarchies and Levels of a Dimension.");
            getHierarchiesAndLevels(mdmGlobalSchema);

            log.info("Getting the MdmAttribute Objects of an MdmPrimaryDimension.");
            getAttributes(mdmGlobalSchema);
        } catch (Exception e) {
            log.error("Cannot get the MDM metadata provider.", e);
        }

        // Now close the data provider and the connection.
        log.info("Closing the DataProvider.");
        dp.close();

        log.info("Closing the Connection.");
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            log.info("Cannot close the connection.", e);
        }
    }

    private MdmDatabaseSchema getSchemaAndPrintAllSchemas(MdmMetadataProvider mp, String schemaName) {
        MdmRootSchema mdmRootSchema = (MdmRootSchema) mp.getRootSchema();
        List<MdmDatabaseSchema> dbSchemas = mdmRootSchema.getDatabaseSchemas();

        if (!isGlobalAwOnly) {
            for (MdmDatabaseSchema mdmDBSchema : dbSchemas) {
                log.info(mdmDBSchema.getName());
                printOrgSchemas(mdmDBSchema);
            }
        }

        log.info("Getting a Single MdmDatabaseSchema.");

        return mdmRootSchema.getDatabaseSchema(schemaName);
    }

    @SuppressWarnings("unchecked")
    private void printOrgSchemas(MdmSchema mdmSchema) {
        List<MdmOrganizationalSchema> orgSchemaList = new ArrayList<>();

        if (mdmSchema instanceof MdmDatabaseSchema mdmDBSchema) {
            orgSchemaList = (List<MdmOrganizationalSchema>) mdmDBSchema.getOrganizationalSchemas();
        } else if (mdmSchema instanceof MdmOrganizationalSchema mdmOrgSchema) {
            orgSchemaList = (List<MdmOrganizationalSchema>) mdmOrgSchema.getOrganizationalSchemas();
        }

        if (orgSchemaList.size() > 0) {
            log.info("The MdmOrganizationalSchema subschemas of {} are:", mdmSchema.getName());

            for (MdmOrganizationalSchema mdmOrgSchema : orgSchemaList) {
                log.info(mdmOrgSchema.getName());
                printOrgSchemas(mdmOrgSchema);
            }
        } else {
            log.info("{} does not have any MdmOrganizationalSchema subschemas.", mdmSchema.getName());
        }
    }

    @SuppressWarnings("unchecked")
    private void getObjects(MdmDatabaseSchema mdmGlobalSchema) {
        if (!isGlobalAwOnly) {
            List<MdmObject> dimList = mdmGlobalSchema.getDimensions();
            String objName = mdmGlobalSchema.getName() + " schema";
            getNames(dimList, "dimensions", objName);

            List<MdmObject> measList = mdmGlobalSchema.getMeasures();
            getNames(measList, "measures", objName);
        } else {
            List<MdmObject> dimList = globalAw.getDimensions();
            String objName = globalAw.getName() + " analytic workspace";
            getNames(dimList, "dimensions", objName);

            List<MdmCube> cubeList = globalAw.getCubes();
            ArrayList<MdmMeasure> allMeasures = new ArrayList<>();
            for (MdmCube mdmCube : cubeList) {
                List<MdmMeasure> measList = mdmCube.getMeasures();
                allMeasures.addAll(measList);
            }
            getNames(allMeasures, "measures", objName);
        }
    }

    private void getNames(List<? extends MdmObject> objectList, String objTypes, String objName) {
        log.info("The {} of the {} are:", objTypes, objName);

        for (MdmObject mdmObject : objectList) {
            log.info("   {}", mdmObject.getName());
        }
    }

    @SuppressWarnings("unchecked")
    private void getCubeObjects(MdmDatabaseSchema mdmGlobalSchema) {
        MdmCube mdmUnitsCube = (MdmCube) mdmGlobalSchema.getTopLevelObject("PRICE_CUBE");
        String objName = mdmUnitsCube.getName() + " cube";
        List<MdmObject> dimList = mdmUnitsCube.getDimensions();
        getNames(dimList, "dimensions", objName);

        List<MdmMeasure> measList = mdmUnitsCube.getMeasures();
        getNames(measList, "measures", objName);
    }

    @SuppressWarnings("unchecked")
    private void getHierarchiesAndLevels(MdmDatabaseSchema mdmGlobalSchema) {
        MdmPrimaryDimension mdmCustomerDimension = (MdmPrimaryDimension) mdmGlobalSchema.getTopLevelObject("CUSTOMER");
        List<MdmHierarchy> hierList = mdmCustomerDimension.getHierarchies();

        log.info("The hierarchies of the {} dimension are:", mdmCustomerDimension.getName());

        for (MdmHierarchy mdmHierarchy : hierList) {
            log.info("   {}", mdmHierarchy.getName());

            if (mdmHierarchy instanceof MdmLevelHierarchy mdmLevelHierarchy) {
                List<MdmHierarchyLevel> hierLevelList = mdmLevelHierarchy.getHierarchyLevels();

                log.info("      The levels of the hierarchy are:");

                for (MdmHierarchyLevel mdmHierLevel : hierLevelList) {
                    log.info("      {}", mdmHierLevel.getName());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void getAttributes(MdmDatabaseSchema mdmGlobalSchema) {
        MdmTimeDimension mdmTimeDimension = (MdmTimeDimension) mdmGlobalSchema.getTopLevelObject("TIME");
        List<MdmAttribute> attrList = mdmTimeDimension.getAttributes();

        log.info("The MdmAttribute objects of {} are:", mdmTimeDimension.getName());

        for (MdmAttribute mdmAttribute : attrList) {
            log.info("   {}", mdmAttribute.getName());
        }

        MdmAttribute mdmParentAttribute = mdmTimeDimension.getParentAttribute();
        log.info("The parent attribute is {}.", mdmParentAttribute.getName());
    }

    public void execute(String[] args) {
        createContext = false;
        super.execute(convertToProperties(args));
    }

    public void setGlobalAwOnly(boolean isGlobalAwjOnly) {
        this.isGlobalAwOnly = isGlobalAwjOnly;
    }

    private void setGlobalAw(MdmDatabaseSchema mdmGlobalSchema, String awName) {
        List<AW> aws = mdmGlobalSchema.getAWs();

        for (AW aw : aws) {
            if (aw.getName().equals(awName)) {
                globalAw = aw;
                break;
            }
        }
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new ConnectAndDiscover().execute(args);
    }
}
