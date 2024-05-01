package acme.oracledb;

import oracle.jdbc.pool.OracleDataSource;
import oracle.olapi.data.cursor.CompoundCursor;
import oracle.olapi.data.cursor.Cursor;
import oracle.olapi.data.cursor.CursorManager;
import oracle.olapi.data.cursor.ValueCursor;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.data.source.FundamentalMetadataProvider;
import oracle.olapi.data.source.Source;
import oracle.olapi.metadata.mdm.*;
import oracle.olapi.session.UserSession;
import oracle.olapi.transaction.NotCommittableException;
import oracle.olapi.transaction.TransactionProvider;

import java.sql.SQLException;
import java.util.*;

/**
 * Creates and stores various objects that are commonly used by the example
 * programs.
 * Creates a Properties object and adds to it the command-line arguments.
 * Uses the Properties object in creating a connection to an Oracle Database
 * instance.
 * Gets metadata objects and stores them for use by example programs.
 * Has methods for getting the stored objects, for committing the current
 * Transaction, and for creating Cursor objects to display query results.
 * <p>
 * This class is created by a BaseExample instance.
 */
public class Context {
    /**
     * The CursorPrintWriter for this session.
     */
    private CursorPrintWriter cpw = null;

    /**
     * The Logger for this session.
     */
    private Logger logOutput = null;

    /**
     * The TransactionProvider for this session.
     */
    private TransactionProvider tp = null;
    /**
     * The DataProvider for this session.
     */
    private DataProvider dp = null;

    /**
     * The Oracle JDBC OracleConnection for this session.
     */
    private oracle.jdbc.OracleConnection conn = null;

    /**
     * The MdmMetadataProvider for this session.
     */
    private MdmMetadataProvider mp = null;

    /**
     * The Properties object for storing the command-line arguments and other
     * parameters to use when creating the connection to the database.
     */
    private Properties props = new Properties();

    /**
     * The HashMap to store the metadata objects and their names.
     */
    private Map m_hashMap = null;

    private List m_dimensionList = null;
    private List m_measureList = null;

    private String _server;
    private String _user;

    /**
     * Creates a new Context11g object.
     */
    public Context(Properties properties, Logger logger) {
        props = properties;
        logOutput = logger;
        cpw = new CursorPrintWriter(logger.getPrintWriter());

        // Connect to the database.
        _connect();
    }

    /**
     * Closes the DataProvider and the connection to the Oracle Database instance.
     */
    public void close() {
        _disconnect();
    }

    /**
     * Makes the connection to the Oracle OLAP server.
     */
    private void _connect() {
        showInfo("Connecting to the database.");
        showInfo("Loading the JDBC driver.");

        try {
            Class.forName("oracle.jdbc.OracleDriver");
            showInfo("JDBC driver loaded.");
        } catch (Exception e) {
            showError("Cannot not load the JDBC driver. " + e);
        }

        showInfo("Setting up connection properties.");

        // From the Properties object, get the command-line arguments that
        // specify the URL and the username and password to use in creating
        // the connection to the Oracle database.
        // The URL has the following information:
        //    An Oracle JDBC driver
        //    The server running the Oracle database
        //    The number of the port on which Oracle OLAP is listening
        //    The system identifier (SID) of the Oracle instance
        // When running any of the example programs, specify a URL, a username and
        // a password as command-line arguments in the following format:
        //   -url jdbc:oracle:thin:@serverName:portNumber:sid -user username
        //   -password password
        // In the URL specification, replace  "serverName" with the hostname of
        // the server on which the Orcle database is running.
        // Replace "portNumber" with the number of the TCP/IP listener port for
        // the database (which is 1521 by default).
        // Replace "sid" with the system identifier (SID) of the Oracle instance
        // to which you want to connect.
        // An example url is "jdbc:oracle:thin:@myOracleServer:1521:orcl".
        // The example programs use the Global schema. To run an example program
        // specify the username "global" and the password for the global user.
        // An example of the command-line arguments is the following:
        //  -url jdbc:oracle:thin:@myOracleServer:1521:orcl -user global
        //  -password whA20eV5r

        String url = props.getProperty("url");
        String user = props.getProperty("user");
        String password = props.getProperty("password");

        int indexAtSign = url.indexOf("@");
        int indexColonAfterServer = url.indexOf(":", indexAtSign);
        String server = url.substring(indexAtSign + 1, indexColonAfterServer);
        setServer(server);
        setUser(user);

        showInfo("Connecting to " + getServer() + " as " + getUser() + ".");
        showInfo("Getting the connection.");

        try {
            OracleDataSource ods = new OracleDataSource();
            ods.setURL(props.getProperty("url"));
            ods.setUser(props.getProperty("user"));
            ods.setPassword(props.getProperty("password"));
            conn = (oracle.jdbc.OracleConnection) ods.getConnection();
        } catch (SQLException e) {
            System.out.println("Connection attempt failed. " + e);
        }

        _createProviders();
    }

    /**
     * Closes the DataProvider.
     */
    public void _disconnect() {
        if (null != dp) {
            showInfo("Closing DataProvider");
            dp.close();
            dp = null;
        }
        _closeConnection();
    }

    /**
     * Closes the connection to the Oracle Database instance.
     */
    private void _closeConnection() {
        try {
            if (null != conn) {
                showInfo("Closing JDBC connection");
                conn.close();
            }
        } catch (Exception e) {
            // Ignore.
        } finally {
            conn = null;
        }
    }

    /**
     * Creates and stores the DataProvider, TransactionProvider, and the
     * MdmMetadataProvider.
     */
    private void _createProviders() {
        showInfo("Creating a DataProvider.");
        dp = new DataProvider();

        try {
            UserSession session = dp.createSession(conn);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        showInfo("Creating a TransactionProvider.");
        tp = dp.getTransactionProvider();

        // Get an MdmMetadataProvider.
        showInfo("Getting the MDM MetadataProvider.");
        try {
            mp = (MdmMetadataProvider) dp.getMdmMetadataProvider();
        } catch (Exception e) {
            showError("Cannot create the MDM metadata provider." + e);
        }
    }

    /**
     * Gets the stored DataProvider.
     *
     * @return The stored DataProvider.
     */
    public DataProvider getDataProvider() {
        return dp;
    }

    /**
     * Gets the FundamentalMetadataProvider.
     *
     * @return The FundamentalMetadataProvider associated with the DataProvider.
     */
    public FundamentalMetadataProvider getFundamentalMetadataProvider() {
        return dp.getFundamentalMetadataProvider();
    }

    /**
     * Gets the stored MdmMetadataProvider.
     *
     * @return The MdmMetadataProvider associated with the DataProvider.
     */
    public MdmMetadataProvider getMetadataProvider() {
        return mp;
    }

    /**
     * Gets the stored TransactionProvider.
     *
     * @return The stored TransactionProvider.
     */
    public TransactionProvider getTransactionProvider() {
        return tp;
    }

    /**
     * From the top-level MdmSchema, gets the subschemas.
     * If the user is GLOBAL, then the method looks for the GLOBALAWJ
     * subschema and makes it the schema to use.
     * If the subschema does not exist, then it uses the MdmDatabaseSchema
     * for the GLOBAL user.
     * If the user is not GLOBAL, then it uses the MdmDatabaseSchema
     * for the current user.
     * The method gets a list of MdmDimension objects and a list of MdmMeasure
     * objects from the MdmDatabaseSchema.
     * From the lists, the method creates a hash map to store the names of the
     * dimensions and measures.
     */
    private Map loadHashMap() {
        // Get the schema for the current user.
        MdmRootSchema mdmRootSchema = (MdmRootSchema) mp.getRootSchema();
        MdmSchema mdmSchema = mdmRootSchema.getDatabaseSchema(getUser());

        // Create an empty HashMap.
        Map map = new HashMap();

        boolean isGlobal = false;
        MdmSchema mdmSchemaToUse = null;

        if (getUser().toUpperCase().equals("GLOBAL")) {
            isGlobal = true;

            List subSchemas = mdmSchema.getSubSchemas();
            Iterator subSchemasItr = subSchemas.iterator();

            while (subSchemasItr.hasNext()) {
                MdmOrganizationalSchema schemaToUse =
                        (MdmOrganizationalSchema) subSchemasItr.next();
                if (mdmSchemaToUse.getName().equals("GLOBAL_AWJ")) {
                    mdmSchema = mdmSchemaToUse;
                    break;
                }
            }
        }

        // Get the dimension list.
        m_dimensionList = mdmSchema.getDimensions();
        Iterator objIter = m_dimensionList.iterator();
        MdmPrimaryDimension mdmPDim = null;

        // Get the dimensions and put them in the hash map.
        while (objIter.hasNext()) {
            mdmPDim = (MdmPrimaryDimension) objIter.next();
            // Put the upper case name of the dimension into the hash map as the key
            // with the object as the value.
            map.put(mdmPDim.getName().toUpperCase(), mdmPDim);
        }

        // Get the measure list.
        if (isGlobal) {
            m_measureList = mdmSchema.getMeasures();
        } else {
            MdmMeasureDimension mDim =
                    (MdmMeasureDimension) mdmSchema.getMeasureDimension();
            m_measureList = mDim.getMeasures();
        }

        objIter = m_measureList.iterator();

        MdmMeasure mdmMeasure = null;
        // Put the measures in the hash map.
        while (objIter.hasNext()) {
            mdmMeasure = (MdmMeasure) objIter.next();
            // Put upper case name of the measure into the hash map as the key
            // with the object as the value.
            map.put(mdmMeasure.getName().toUpperCase(), mdmMeasure);
        }

        return map;
    }

    /**
     * Gets the MdmMeasure that has the specified name.
     *
     * @param name The name of the MdmMeasure that you want.
     * @return The MdmMeasure that has the specified name or null.
     */
    public MdmMeasure getMdmMeasureByName(String name) {
        // If the hash map has not been created yet, create it.
        if (m_hashMap == null)
            m_hashMap = loadHashMap();

        MdmMeasure result = null;

        // If the hash map load succeeded, then get the measure.
        if (m_hashMap != null) {
            String val = name.toUpperCase();
            result = (MdmMeasure) m_hashMap.get(val);
        }
        return result;
    }

    /**
     * Gets an array of MdmMeasure objects that has one MdmMeasure for each
     * specified name.
     * If the name is not in the hash map of MdmObject objects, then the array
     * element for that name is null.
     *
     * @param names The names of the MdmMeasure objects that you want.
     * @return An array of the MdmMeasure objects that have the specified names.
     */
    public MdmMeasure[] getMdmMeasuresByName(String[] names) {
        // If the hash map has not been created yet, then create it.
        if (m_hashMap == null)
            m_hashMap = loadHashMap();

        // Create an array of nulls.
        MdmMeasure[] mdmMeasures = new MdmMeasure[names.length];
        // If the hash map load succeeded, then get the MdmMeasure objects.
        if (m_hashMap != null) {
            // Loop over the names...
            for (int i = 0; i < names.length; i++) {
                // Get the HashMap value for this name.
                String val = names[i].toUpperCase();
                mdmMeasures[i] = (MdmMeasure) m_hashMap.get(val);
            }
        }
        return (mdmMeasures);
    }

    /**
     * Gets the MdmPrimaryDimension that has the specified name.
     *
     * @param name The name of the MdmPrimaryDimension that you want.
     * @return The MdmPrimaryDimension that has the specified name or null.
     */
    public MdmPrimaryDimension getMdmPrimaryDimensionByName(String name) {
        // If the hash map has not been created yet, then create it.
        if (m_hashMap == null)
            m_hashMap = loadHashMap();

        MdmPrimaryDimension result = null;

        // If the hash map load succeeded, then get the dimension.
        if (m_hashMap != null) {
            // Loop over the names.
            String val = name.toUpperCase();
            // Get the MdmPrimaryDimension with that name.
            result = (MdmPrimaryDimension) m_hashMap.get(val);
        }
        return result;
    }

    /**
     * Gets the specified MdmAttribute.
     *
     * @param mdmPDim The MdmPrimaryDimension associated with the attribute.
     * @param name    The name of the attribute.
     * @return The MdmAttribute with the specified name.
     */
    public MdmAttribute getAttributeByName(MdmPrimaryDimension mdmPDim,
                                           String name) {
        List mdmAttrs = mdmPDim.getAttributes();
        Iterator mdmAttrsItr = mdmAttrs.iterator();
        while (mdmAttrsItr.hasNext()) {
            MdmAttribute mdmAttr = (MdmAttribute) mdmAttrsItr.next();
            if (mdmAttr.getName().equals(name))
                return mdmAttr;
        }
        return null;
    }

    /**
     * Gets the specified MdmHierarchy.
     *
     * @param mdmPDim The MdmPrimaryDimension associated with the hierarchy.
     * @param name    The name of the hierarchy.
     * @return The hierarchy with the specified name.
     */
    public MdmHierarchy getHierarchyByName(MdmPrimaryDimension mdmPDim,
                                           String name) {
        List mdmHiers = mdmPDim.getHierarchies();
        Iterator mdmHierItr = mdmHiers.iterator();
        while (mdmHierItr.hasNext()) {
            MdmHierarchy mdmHier = (MdmHierarchy) mdmHierItr.next();
            if (mdmHier.getName().equals(name))
                return mdmHier;
        }
        return null;
    }

    /**
     * Gets the MdmHierarchyLevel that has the specified name.
     *
     * @param mdmLvlHier The MdmLevelHierarchy that contains the level.
     * @param levelName  The name of the level.
     * @return The MdmHierarchyLevel that has the specified name.
     */
    public MdmHierarchyLevel getHierLevelByName(MdmLevelHierarchy mdmLvlHier,
                                                String levelName) {
        List<MdmHierarchyLevel> hierList = mdmLvlHier.getHierarchyLevels();
        for (MdmHierarchyLevel mdmHier : hierList) {
            List<MdmHierarchyLevel> hierLevelList =
                    mdmLvlHier.getHierarchyLevels();
            for (MdmHierarchyLevel mdmHierLevel : hierLevelList) {
                if (mdmHierLevel.getName().equals(levelName)) {
                    return mdmHierLevel;
                }
            }
        }

        return null;
    }

    /**
     * Gets the local value portion of the unique value.
     *
     * @param uniqueValue The unique value from which to extract the local value.
     * @return The local value portion of the unique value.
     */
    public String getLocalValue(String uniqueValue) {
        return cpw.getLocalValue(uniqueValue);
    }

    /**
     * Displays a line of text.
     *
     * @param line A String that contains the text that you want to display.
     */
    public void println(String line) {
        logOutput.println(line);
    }

    /**
     * Calls the printCursor(Cursor rootCursor, boolean displayLocVal) method
     * of the CursorPrintWriter and passes it the rootCursor and the boolean
     * value false.
     * The boolean value specifies that if a ValueCursor has Dimension values,
     * the method displays the full unique value rather than the local value.
     *
     * @param rootCursor The Cursor whose values you want to display.
     */
    public void printCursor(Cursor rootCursor) {
        cpw.printCursor(rootCursor, false);
    }

    /**
     * Displays the values of a CompoundCursor in a crosstab format.
     *
     * @param rootCursor The CompoundCursor to display.
     */
    public void printAsCrosstab(CompoundCursor rootCursor) {
        cpw.printAsCrosstab(rootCursor);
    }

    /**
     * Displays the specified message.
     *
     * @param message A String that contains the message to display.
     */
    public void showInfo(String message) {
        logOutput.showInfo(message);
    }

    /**
     * Displays the specified error message.
     *
     * @param message A String that contains the error message to display.
     */
    public void showError(String message) {
        logOutput.showError(message);
    }

    /**
     * Prepares and commits the current Transaction.
     */
    public void commit() {
        try {
            tp.prepareCurrentTransaction();
        } catch (NotCommittableException e) {
            println("Cannot prepare the current Transaction. " + e);
        }
        tp.commitCurrentTransaction();
    }


    /**
     * Creates a Cursor for the specified Source and displays the values of
     * the Cursor, with unique values for the dimension elements.
     */
    public void displayResult(Source source) {
        _displayResult(source, false);
    }

    /**
     * Creates a Cursor for the specified Source and displays the values of
     * the Cursor, with unique values for the dimension elements if displayLocVal
     * is false and with local values only if displayLocVal is true.
     * This method retrieves local values only when the value separation String
     * is the default double colons (::).
     *
     * @param source        The Source for which you want to create a Cursor and
     *                      display its values.
     * @param displayLocVal A boolean that specifies whether to display
     *                      unique or local dimension element values.
     */
    private void _displayResult(Source source, boolean displayLocVal) {
        CursorManager cursorManager =
                dp.createCursorManager(source);
        Cursor cursor = cursorManager.createCursor();

        cpw.printCursor(cursor, displayLocVal);

        // Close the CursorManager.
        cursorManager.close();
    }


    /**
     * Displays the values of the specified Cursor.
     * This method displays the unique value of dimension elements.
     *
     * @param cursor The Cursor that has the values you want to display.
     */
    public void displayCursor(Cursor cursor) {
        _displayCursor(cursor, false);
    }

    /**
     * Displays the values of the specified Cursor.
     *
     * @param cursor        The Cursor that has the values you want to display.
     * @param displayLocVal A boolean that specifies whether to display
     *                      unique or local dimension element values.
     */
    public void displayCursor(Cursor cursor, boolean displayLocVal) {
        _displayCursor(cursor, displayLocVal);
    }

    /**
     * Displays the values of the specified Cursor.
     *
     * @param cursor        The Cursor that has the values you want to display.
     * @param displayLocVal A boolean that specifies whether to display
     *                      unique or local dimension element values.
     */
    private void _displayCursor(Cursor cursor, boolean displayLocVal) {
        cpw.printCursor(cursor, displayLocVal);
    }

    /**
     * Creates a Cursor for the Source produced by a TopBottomTemplate and
     * displays the values of the Cursor.
     *
     * @param source The Source returned by the getSource method of a
     *               DynamicDefinition for the TopBottomTemplate example.
     */
    public void displayTopBottomResult(Source source) {
        CursorManager cursorManager =
                dp.createCursorManager(source);
        ValueCursor valueCursor = (ValueCursor) cursorManager.createCursor();

        cpw.printTopBottomResult(valueCursor);

        // Close the CursorManager.
        cursorManager.close();
    }

    /**
     * Displays the values of a Cursor in a crosstab format.
     *
     * @param cursor The CompoundCursor to display.
     */
    public void displayCursorAsCrosstab(CompoundCursor cursor) {
        cpw.printAsCrosstab(cursor);
    }

    /**
     * Specifies the name of the Oracle OLAP server for the connection.
     *
     * @param server The name of the server on which the Oracle Database
     *               instance is running.
     */
    public void setServer(String server) {
        _server = server;
    }

    /**
     * Gets the name of the Oracle OLAP server for the connection.
     *
     * @return A String containing the name of the Oracle OLAP server.
     */
    public String getServer() {
        return _server;
    }

    /**
     * Specifies the username for the connection.
     *
     * @param user The username to use in creating the connection.
     */
    public void setUser(String user) {
        _user = user.toUpperCase();
    }

    /**
     * Gets the username to use for the connection.
     *
     * @return A String containing the username for creating the connection.
     */
    public String getUser() {
        return _user;
    }

    /**
     * Gets the Properties object that contains the command-line arguments for
     * this example.
     *
     * @return Properties object that contains the command-line arguments for
     * this example.
     */
    public Properties getProperties() {
        return props;
    }

    /**
     * Gets a runtime property (a command-line argument).
     *
     * @param key      The name of the property.
     * @param required If this flag is true and the property is not specified,
     *                 then this method throws an IllegalArgumentException with
     *                 an appropriate message.
     */
    public String getProperty(String key, boolean required) {
        String ret = props.getProperty(key);
        if ((required == true) && (ret == null)) {
            throw new IllegalArgumentException("Command-line arguments must be " +
                    "specified in the form -<property_name> <value> with " +
                    "the property name and value separated by whitespace.");
        }
        return ret;
    }

    /**
     * Gets the value of the property with the specified key.
     *
     * @param key The key of the property for which you want the value.
     * @return The value associated with the specified key.
     */
    public String getOptionalProperty(String key) {
        return getProperty(key, false);
    }
}
