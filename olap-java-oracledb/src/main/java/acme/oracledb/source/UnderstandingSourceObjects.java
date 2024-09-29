package acme.oracledb.source;

import acme.oracledb.BaseExample;
import oracle.olapi.data.cursor.Cursor;
import oracle.olapi.data.cursor.CursorManager;
import oracle.olapi.data.cursor.NoDataAvailableException;
import oracle.olapi.data.source.*;
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
        usingDerivedSourceObjectsToSelectMeasureValues();

        // The "Extracting Elements of a Source" example.
        extractingElementsOfASource();

        // The "Using a Parameterized Source to Change a Dimension Selection" example.
        usingAParameterizedSource();
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

        log.info("prodDim is");
        getContext().displayResult(prodDim);

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

    public void usingDerivedSourceObjectsToSelectMeasureValues() {
        log.info("Using Derived Source Objects to Select Measure Values");

        // Create lists of product and time dimension members.
        Source productsToSelect = dp.createListSource(new String[]
                {"PRIMARY::ITEM::ITEM_ENVY EXE",
                        "PRIMARY::ITEM::ITEM_ENVY STD"});

//        log.info("productsToSelect is");
//        getContext().displayResult(productsToSelect);

        Source timesToSelect = dp.createListSource(new String[]
                {"CALENDAR::MONTH::MONTH_2000.01",
                        "CALENDAR::MONTH::MONTH_2001.01",
                        "CALENDAR::MONTH::MONTH_2002.01"});

//        log.info("timesToSelect is");
//        getContext().displayResult(timesToSelect);

        // Get the PRICE_CUBE cube.
        MdmCube mdmPriceCube = mdmDBSchema.findOrCreateCube("PRICE_CUBE");
        // Get the UNIT_PRICE measure from the cube.
        MdmBaseMeasure mdmUnitPrice = mdmPriceCube.findOrCreateBaseMeasure("UNIT_PRICE");
        // Get the PRODUCT and TIME dimensions.
        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        MdmTimeDimension mdmTimeDim = mdmDBSchema.findOrCreateTimeDimension("TIME");

        // Get the Source objects for the dimensions and the measure.
        Source prodDim = mdmProdDim.getSource();
        Source timeDim = mdmTimeDim.getSource();
        Source unitPrice = mdmUnitPrice.getSource();

        // Using the value method, derive Source objects that specify the selected
        // dimension members.
        Source selectedProducts = prodDim.join(prodDim.value(),
                productsToSelect,
                Source.COMPARISON_RULE_SELECT,
                false);
        Source selectedTimes = timeDim.join(timeDim.value(),
                timesToSelect,
                Source.COMPARISON_RULE_SELECT,
                false);

        // Derive a Source that specifies the unitPrice values for the selected
        // products and times.
        Source pricesForSelectedProductsAndTimes = unitPrice.join(selectedProducts)
                .join(selectedTimes);

        getContext().commit();
        getContext().displayResult(pricesForSelectedProductsAndTimes);
    }

    public void extractingElementsOfASource() {
        log.info("Extracting Elements of a Source");

        MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
        StringSource prodDim = (StringSource) mdmProdDim.getSource();
        Source productsToSelect = prodDim.selectValues(new String[]
                {"PRIMARY::ITEM::ITEM_ENVY ABM",
                        "PRIMARY::ITEM::ITEM_ENVY EXE",
                        "PRIMARY::ITEM::ITEM_ENVY STD"});
        Source moreProductsToSelect = prodDim.selectValues(new String[]
                {"PRIMARY::ITEM::ITEM_SENT FIN",
                        "PRIMARY::ITEM::ITEM_SENT MM",
                        "PRIMARY::ITEM::ITEM_SENT STD"});
        Source sourcesToCombine = dp.createListSource(new Source[]{productsToSelect, moreProductsToSelect});

        Source sourcesToCombineWithAnInput = sourcesToCombine.extract();
        Source combinedProducts = sourcesToCombineWithAnInput.joinHidden(sourcesToCombine);

        getContext().commit();
        getContext().displayResult(combinedProducts);
    }

    public void usingAParameterizedSource() {
        try {
            log.info("Using a Parameterized Source to Change a Dimension Selection");

            // Get the PRODUCT dimension and the Source for it.
            MdmStandardDimension mdmProdDim = mdmDBSchema.findOrCreateStandardDimension("PRODUCT");
            StringSource prodDim = (StringSource) mdmProdDim.getSource();

            // Create a parameterized Source that specifies a member of the dimension.
            StringParameter prodParam = new StringParameter(dp, "PRIMARY::FAMILY::FAMILY_LTPC");
            Source prodParamSrc = prodParam.createSource();
            Source paramProdSel = prodDim.join(prodDim.value(), prodParamSrc);

            // Get the local value attribute of the dimension.
            Source locValAttr = mdmProdDim.getLocalValueAttribute().getSource();

            // Get the local value for the specified dimension member.
            Source dimMemberWithLocalValue = locValAttr.join(paramProdSel);

            // Commit the current Transaction.
            getContext().commit();

            // Display the local value for the specified member.
            log.info("The local value of the dimension member is:");

            CursorManager cursorMngr = dp.createCursorManager(dimMemberWithLocalValue);
            Cursor cursor = cursorMngr.createCursor();
            getContext().displayCursor(cursor);

            // Change the product parameter value.
            prodParam.setValue("PRIMARY::FAMILY::FAMILY_DTPC");

            // Reset the Cursor position to 1
            cursor.setPosition(1);

            // Display the local value for the member that is now specified.
            log.info("The local value of the dimension member after changing the " +
                    "Parameter value is:");
            getContext().displayCursor(cursor);
        } catch (NoDataAvailableException ex) {
            log.info("No data available.");
        }
    }

    public static void main(String[] args) {
        // Run with command line arguments (url, user and password)
        // For example: -url jdbc:oracle:thin:@//192.168.206.128:1521/orclpdb.localdomain -user global -password password
        new UnderstandingSourceObjects().execute(args);
    }
}
