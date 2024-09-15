package acme.oracledb.source;

import acme.oracledb.BaseExample;
import oracle.olapi.data.source.DataProvider;
import oracle.olapi.data.source.NumberSource;
import oracle.olapi.data.source.Source;
import oracle.olapi.data.source.StringSource;
import oracle.olapi.metadata.mdm.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

        // The "Using the isSubtypeOf Method" example.
        usingTheisSubtypeOfMethod();

        // Informal example.
        gettingTheIDofASource();

        // The "A Simple Join That Produces a Source with an Output" example.
        simpleJoinProducingSourceWithOutput();

        // The "A Simple Join That Selects Elements of the Joined Source" example.
        simpleJoinSelectingElementsOfTheJoinedSource();

        // The "A Simple Join That Removes Elements of the Joined Source" example.
        simpleJoinRemovingElementsOfTheJoinedSource();

        // The "A Simple Join That Produces a Source with Two Outputs" example.
        List<Source> lettersWithSelNames_Colors = simpleJoinProducingSourceWithTwoOutputs();

        // The "A Simple Join That Hides An Output" example.
        simpleJoinHidingAnOutput(lettersWithSelNames_Colors);

        // The "Getting an Attribute for a Dimension Member" example.
        gettingAttributeForDimMember();

        // The "Getting Measure Values" example.
        gettingMeasureValues();

        // The "Using the value Method to Relate a Source to Itself" exrample.
        usingValueToRelatingSourceToItself();

        // The "Using the value Method to Select Elements of a Source" example.
        usingValueToSelectElements();

        // The "Using Derived Source Objects to Select Measure Values" example.
//        usingDerivedSourceObjectsToSelectMeasureValues();

        // The "Extracting Elements of a Source" example.
//        extractingElementsOfASource();

        // The "Using a Parameterized Source to Change a Dimension Selection" example.
//        usingAParameterizedSource();
    }

    public void gettingTheMembersOfADimension() {
        // Informal example in the Elements and Values of a Source topic of Oracle OLAP Java API Developer's Guide.

        MdmStandardDimension mdmChanDim = mdmDBSchema.findOrCreateStandardDimension("CHANNEL");
        Source chanDim = mdmChanDim.getSource();

        // We do not need to commit the Transaction because we did not create a derived Source.
        log.info("Informal example that gets the values of the elements of the Source for the {} dimension.", mdmChanDim.getName());
        getContext().displayResult(chanDim);
    }

    public void usingTheisSubtypeOfMethod() {
        log.info("Using the isSubtypeOf Method");

        Source myList = getDataProvider().createListSource(new String[]
                {"PRODUCT_PRIMARY::FAMILY::LTPC",
                        "PRODUCT_PRIMARY::FAMILY::DTPC",
                        "PRODUCT_PRIMARY::FAMILY::ACC",
                        "PRODUCT_PRIMARY::FAMILY::MON"});

        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        MdmHierarchy mdmProdHier = mdmProdDim.getDefaultHierarchy();
        StringSource prodHier = (StringSource) mdmProdHier.getSource();

        // Selecting values using the selectValues shortcut.
        Source prodSel = prodHier.selectValues(myList);
        if (prodSel.isSubtypeOf(prodHier)) {
            log.info("   prodSel is a subtype of prodHier.");
        } else {
            log.info("   prodSel is not a subtype of prodHier.");
        }
    }

    private void gettingTheIDofASource() {
        // Informal example that appears in the Source Identification and
        // SourceDefinition of a Source topic of Oracle OLAP Java API
        // Developer's Guide.

        log.info("Informal example that gets the ID of a Source.");

        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        Source prodDim = mdmProdDim.getSource();

        log.info("   The Source ID of prodDim is {}", prodDim.getID());
    }

    public void simpleJoinProducingSourceWithOutput() {
        log.info("A Simple Join That Produces a Source with an Output");

        Source letters = dp.createListSource(new String[]{"A", "B", "C"});
        Source names = dp.createListSource(new String[]{"Stephen", "Leo", "Molly"});
        Source lettersWithNames = letters.join(names);

        getContext().displayResult(lettersWithNames);
    }

    public void simpleJoinSelectingElementsOfTheJoinedSource() {
        log.info("A Simple Join That Selects Elements of the Joined Source");

        Source letters = dp.createListSource(new String[]{"A", "B", "C"});
        Source names = dp.createListSource(new String[]{"Stephen", "Leo", "Molly"});

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        Source someNames = dp.createListSource(new String[]{"Stephen", "Molly"});
        Source lettersAndSelectedNames = letters.join(names, someNames, Source.COMPARISON_RULE_SELECT, true);

        getContext().commit();
        getContext().displayResult(lettersAndSelectedNames);
    }

    public void simpleJoinRemovingElementsOfTheJoinedSource() {
        log.info("A Simple Join That Removes Elements of the Joined Source");

        Source letters = dp.createListSource(new String[]{"A", "B", "C"});
        Source names = dp.createListSource(new String[]{"Stephen", "Leo", "Molly"});
        Source someNames = dp.createListSource(new String[]{"Stephen", "Molly"});

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        Source lettersAndNamesWithoutRemovedNames = letters.join(names, someNames, Source.COMPARISON_RULE_REMOVE, true);

        getContext().commit();
        getContext().displayResult(lettersAndNamesWithoutRemovedNames);
    }

    public List<Source> simpleJoinProducingSourceWithTwoOutputs() {
        log.info("A Simple Join That Produces a Source with Two Outputs");

        Source letters = dp.createListSource(new String[]{"A", "B", "C"});
        Source names = dp.createListSource(new String[]{"Stephen", "Leo", "Molly"});
        Source someNames = dp.createListSource(new String[]{"Stephen", "Molly"});

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        Source colors = dp.createListSource(new String[]{"Green", "Maroon"});

        Source lettersWithSelectedNames =
                letters.join(names,
                        someNames,
                        Source.COMPARISON_RULE_SELECT,
                        true);
        Source lettersWithSelectedNamesAndColors = lettersWithSelectedNames.join(colors);

        getContext().commit();
        getContext().displayResult(lettersWithSelectedNamesAndColors);

        ArrayList<Source> lettersWithSelNames_Colors = new ArrayList<>();
        lettersWithSelNames_Colors.add(lettersWithSelectedNames);
        lettersWithSelNames_Colors.add(colors);

        return lettersWithSelNames_Colors;
    }

    public void simpleJoinHidingAnOutput(List<Source> lettersWithSelNames_Colors) {
        log.info("A Simple Join That Hides An Output");

        Source lettersWithSelectedNames = lettersWithSelNames_Colors.get(0);
        Source colors = lettersWithSelNames_Colors.get(1);

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        Source lettersWithSelectedNamesAndHiddenColors = lettersWithSelectedNames.joinHidden(colors);

        Source base = lettersWithSelectedNames;
        Source joined = colors;
        Source result = base.join(joined,
                dp.getEmptySource(),
                Source.COMPARISON_RULE_REMOVE,
                false);

        getContext().commit();
        getContext().displayResult(lettersWithSelectedNamesAndHiddenColors);

        log.info("The result of the same query using the full join signature of the joinHidden method.");
        getContext().displayResult(result);
    }

    public void gettingAttributeForDimMember() {
        log.info("Getting an Attribute for a Dimension Member");

        MdmStandardDimension mdmChanDim = mdmDBSchema.findOrCreateStandardDimension("CHANNEL");
        Source chanDim = mdmChanDim.getSource();
        Source locValAttr = mdmChanDim.getLocalValueAttribute().getSource();
        Source dimMembersWithLocalValue = locValAttr.join(chanDim);

        getContext().commit();
        getContext().displayResult(dimMembersWithLocalValue);
    }

    public void gettingMeasureValues() {
        log.info("Getting Measure Values");

        MdmCube mdmPriceCube = mdmDBSchema.findOrCreateCube("PRICE_CUBE");
        MdmBaseMeasure mdmUnitPrice = mdmPriceCube.findOrCreateBaseMeasure("UNIT_PRICE");

        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        MdmTimeDimension mdmTimeDim = mdmDBSchema.findOrCreateTimeDimension("TIME");

        Source prodDim = mdmProdDim.getSource();
        Source timeDim = mdmTimeDim.getSource();
        Source unitPrice = mdmUnitPrice.getSource();

        Source pricesByProductAndTime = unitPrice.join(prodDim).join(timeDim);

//        log.info("pricesByProductAndTime is ");
//        getContext().displayResult(pricesByProductAndTime);

        NumberSource numPricesByProductAndTime = pricesByProductAndTime.count();

        getContext().commit();
        log.info("The number of elements in the numPricesByProductAndTime Source is ");
        getContext().displayResult(numPricesByProductAndTime);
    }

    public void usingValueToRelatingSourceToItself() {
        log.info("Using the value Method to Relate a Source to Itself");

        Source letters = dp.createListSource(new String[]{"A", "B", "C"});
        Source lettersValue = letters.value();
        Source lettersByLettersValue = letters.join(lettersValue);

        getContext().commit();
        getContext().displayResult(lettersByLettersValue);
    }

    public void usingValueToSelectElements() {
        log.info("Using the value Method to Select Elements of a Source");

        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        Source prodDim = mdmProdDim.getSource();

//        log.info("prodDim is");
//        getContext().displayResult(prodDim);

        // The code that appears in the example in
        // Oracle OLAP Java API Developer's Guide begins here.

        Source productsToSelect = dp.createListSource(new String[]
                {"PRIMARY::ITEM::ITEM_ENVY EXE",
                        "PRIMARY::ITEM::ITEM_ENVY STD"});


        Source selectedProducts = prodDim.join(prodDim.value(),
                productsToSelect,
                Source.COMPARISON_RULE_SELECT,
                false); // Hide the output.

        getContext().commit();

        log.info("A Cursor for the productsToSelect Source has the following values.");
        getContext().displayResult(productsToSelect);
        log.info("A Cursor for the selectedProducts Source has the following values.");
        getContext().displayResult(selectedProducts);
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new UnderstandingSourceObjects().execute(args);
    }
}
