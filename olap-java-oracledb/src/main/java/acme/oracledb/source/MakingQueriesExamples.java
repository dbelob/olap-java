package acme.oracledb.source;

import acme.oracledb.BaseExample;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.data.source.NumberSource;
import oracle.olapi.data.source.Source;
import oracle.olapi.data.source.StringSource;
import oracle.olapi.metadata.mdm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MakingQueriesExamples extends BaseExample {
    private static final Logger log = LoggerFactory.getLogger(MakingQueriesExamples.class);

    private DataProvider dp = null;
    private MdmDatabaseSchema mdmDBSchema;

    @Override
    protected void run() throws Exception {
        // Get the DataProvider, MdmMetadataProvider, and the MdmRootSchema.
        dp = getDataProvider();
        MdmMetadataProvider mp = getMdmMetadataProvider();
        MdmRootSchema mdmRootSchema = (MdmRootSchema) mp.getRootSchema();

        // Get the name of the user for the current session.
        String userName = getContext().getUser();

        // Get the MdmDatabaseSchema for the user.
        mdmDBSchema = mdmRootSchema.getDatabaseSchema(userName);

        // The "Controlling Input-with-Source Matching With the alias Method" example.
        controllingMatchingWithAlias();

        //TODO: implement
    }

    private void controllingMatchingWithAlias() throws Exception {
        log.info("Controlling Input-with-Source Matching With the alias Method");

        // Get the cube that contains the UNITS measure.
        MdmCube mdmUnitsCube = mdmDBSchema.findOrCreateCube("UNITS_CUBE");

        // Get the measure.
        MdmBaseMeasure mdmUnits = mdmUnitsCube.findOrCreateBaseMeasure("UNITS");

        // Get the dimensions of the cube.
        MdmTimeDimension mdmTimeDim = mdmDBSchema.findOrCreateTimeDimension("TIME");
        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        MdmStandardDimension mdmCustDim = mdmDBSchema.findOrCreateStandardDimension("CUSTOMER");
        MdmStandardDimension mdmChanDim = mdmDBSchema.findOrCreateStandardDimension("CHANNEL");

        // Get the Source for the measure.
        NumberSource units = (NumberSource) mdmUnits.getSource();

        // Get a hierarchy of each dimension and get the Source for each hierarchy.
        StringSource prodHier = (StringSource) mdmProdDim.findOrCreateLevelHierarchy("PRIMARY").getSource();
        StringSource custHier = (StringSource) mdmCustDim.findOrCreateLevelHierarchy("SHIPMENTS").getSource();
        StringSource chanHier = (StringSource) mdmChanDim.findOrCreateLevelHierarchy("PRIMARY").getSource();
        StringSource timeHier = (StringSource) mdmTimeDim.findOrCreateLevelHierarchy("CALENDAR").getSource();

        // Select single values for the product, customer, and time hierarchies.
        Source prodSel = prodHier.selectValue("PRIMARY::ITEM::ITEM_ENVY ABM");
        Source custSel = custHier.selectValue("SHIPMENTS::SHIP_TO::SHIP_TO_BUSN WRLD SJ");
        Source timeSel = timeHier.selectValue("CALENDAR::MONTH::MONTH_2001.01");

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        // Produce a Source that specifies the units values for the selected
        // dimension values.
        Source unitsSel = units.join(timeSel).join(custSel).join(prodSel);

        // Create aliases for the Channel dimension hierarchy.
        Source chanAlias1 = chanHier.alias();
        Source chanAlias2 = chanHier.alias();

        // Join the aliases to the Source representing the units values specified
        // by the selected dimension elements, using the value method to make the
        // alias an input.
        NumberSource unitsSel1 = (NumberSource) unitsSel.join(chanAlias1.value());
        NumberSource unitsSel2 = (NumberSource) unitsSel.join(chanAlias2.value());

        // chanAlias2 is the first output of result, so the values of it are the
        // row (slower varying) values; chanAlias1 is the second output of result
        // so the values of it are the column (faster varying) values.
        Source result = unitsSel1.gt(unitsSel2)
                .join(chanAlias1)    // Output 2, column
                .join(chanAlias2);   // Output 1, row

        getContext().commit();
        getContext().displayResult(result);
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new oracle.olapi.examples.source.MakingQueriesExamples().execute(args);
    }
}
