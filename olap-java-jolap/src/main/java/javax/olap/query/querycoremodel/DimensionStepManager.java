/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface DimensionStepManager
extends javax.olap.query.querycoremodel.NamedObject {

    // ------------------------------------------------
    // -----   Reference-Generated                -----
    // ------------------------------------------------

  public javax.olap.query.querycoremodel.DimensionView getDimensionView()
    throws javax.olap.OLAPException;

  public void setDimensionView( javax.olap.query.querycoremodel.DimensionView value )
    throws javax.olap.OLAPException;

  public java.util.List getSegment()
    throws javax.olap.OLAPException;

  public java.util.List getDimensionStep()
    throws javax.olap.OLAPException;

    // ------------------------------------------------
    // -----   Interface Operations               -----
    // ------------------------------------------------

  public javax.olap.query.querycoremodel.DimensionStep createDimensionStep( javax.olap.query.enumerations.DimensionStepType stepType )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.DimensionStep createDimensionStepBefore( javax.olap.query.enumerations.DimensionStepType stepType, javax.olap.query.querycoremodel.DimensionStep member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.DimensionStep createDimensionStepAfter( javax.olap.query.enumerations.DimensionStepType stepType, javax.olap.query.querycoremodel.DimensionStep member )
    throws javax.olap.OLAPException;

}
