package acme.oracledb;

import oracle.olapi.data.cursor.*;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Displays the values of an Oracle OLAP Java API Cursor.
 *
 * @author Oracle Corporation
 */
public class CursorPrintWriter extends PrintWriter
{

  /**
    * A required constructor.
    */
  public CursorPrintWriter(PrintWriter dataWriter)
  {
    super(dataWriter, true);
  }

  /**
   * Stores the CursorPrintWriter designated as the standard output device.
   */
  static CursorPrintWriter stdOut = null;

  /**
   * CursorPrintWriter designated as the standard output device.
   *
   * @return The CursorPrintWriter for standard output.
   */
  static CursorPrintWriter getStandardOutput()
  {
    if (null == stdOut)
      stdOut = new UncloseablePrintWriter(System.out);
    return stdOut;
  }

  /**
   * Iterates through the Cursor structure and prints the values of the
   * Cursors.
   * Puts an open parentheses character before the first value in a
   * CompoundCursor and a close parentheses character after the last
   * value in the CompoundCursor.
   * Puts a comma between the values of nested ValueCursor objects.
   * Displays "NA" if the value for a ValueCursor in a tuple has no value.
   * (A tuple is the set of the values of the outputs of a CompoundCursor
   * for a position of the ValueCursor for that CompoundCursor.
   *
   * @param cursor The Cursor whose values you want to display.
   *
   * @param displayLocVal A boolean that specifies whether to display
   *                      unique or local dimension member values.
   */
  private void _printTuple(Cursor cursor, boolean displayLocVal)
  {
    if (cursor instanceof CompoundCursor)
    {
      CompoundCursor compoundCursor = (CompoundCursor)cursor;
      print("(");
      Iterator iterOutputs = compoundCursor.getOutputs().iterator();
      Cursor output = (Cursor)iterOutputs.next();
      _printTuple(output, displayLocVal);
      while (iterOutputs.hasNext())
      {
        print(",");
        _printTuple((Cursor)iterOutputs.next(), displayLocVal);
      }
      print(",");
      _printTuple(compoundCursor.getValueCursor(), displayLocVal);
      print(")");
      _printParentInfo(cursor);
    }
    else if (cursor instanceof ValueCursor)
    {
      ValueCursor valueCursor = (ValueCursor)cursor;
      if (valueCursor.hasCurrentValue())
      {
        if (!displayLocVal)
          print(valueCursor.getCurrentValue());
        else
        {
          int index = 0;
          Object value = valueCursor.getCurrentValue();
          if (value instanceof String)
          {
            String uVal = valueCursor.getCurrentString();
            if (uVal.indexOf("::") > 0)
            {
              index = uVal.lastIndexOf("::");
              String lVal =
                uVal.substring((uVal.lastIndexOf("::") + 2), uVal.length());
              print(lVal);
            }
            else
              print(value);
          }
          else
            print(value);
        }
      }
      else
        print("NA");

      _printParentInfo(cursor);
    }
  }

  /**
   * Displays an integer followed by a colon to indicate the current position
   * of the root CompoundCursor or the ValueCursor.
   * Sends the Cursor to the private method _printTuple, which iterates through
   * the Cursor structure and prints the values of the Cursor objects.
   *
   * @param rootCursor The Cursor whose values you want to display.
   *
   * @param displayLocVal A boolean that specifies whether to display
   *                      unique or local dimension member values.
   */
  public void printCursor(Cursor rootCursor, boolean displayLocVal)
  {
    // The printCursor method has a do...while loop that moves through the
    // positions of the Cursor passed to it. At each position, the method prints
    // the number of the iteration through the loop and then a colon and a
    // space.
    // The method calls the private _printTuple method and then prints a new
    // line. A tuple is the set of output ValueCursor values specified by one
    // position of the parent CompoundCursor. The method prints one line for
    // each position of the parent CompoundCursor.
    int i = 1;
    do
    {
      print(i++ + ": ");
      _printTuple(rootCursor, displayLocVal);
      println();
      flush();
    }
    while (rootCursor.next());
  }

  /**
   * Prints the starting and ending positions of the parent of the current
   * value of Cursor.
   *
   * @param cursor The Cursor for which to get the startingending positions
   *               of the parent.
   */
  private void _printParentInfo(Cursor cursor)
  {
    try
    {
      long parentStart = cursor.getParentStart();
      long parentEnd = cursor.getParentEnd();
      print("{" + parentStart + "," + parentEnd + "}");
    }
    catch (NotSpecifiedException e) // Ignore the Exception.
    {
    }
  }

  /**
   * Prints the results of the TopBottomTest example program.
   *
   * @param valueCursor A ValueCursor that contains the values produced by the
   *                    TopBottomTest example program.
   */
  public void printTopBottomResult(ValueCursor valueCursor)
  {
    int i = 1;
    String count;

    do
    {
      if (i < 10)
        count = (" " + i + ". ");
      else
        count = (i + ". ");

      try
      {
        println(count + Math.round(valueCursor.getCurrentDouble()));
      }
      catch (IncorrectDataTypeException e)
      {
        println(count + valueCursor.getCurrentValue());
      }
      i++;
    }
    while (valueCursor.next());
  }

  /**
   * Prints the values of a CompoundCursor in a crosstab format with
   * columns and rows and with pages as required.
   *
   * @param rootCursor The CompoundCursor that has the values to display
   *                   in a crosstab.
   */
  public void printAsCrosstab(CompoundCursor rootCursor)
  {
    List outputs = rootCursor.getOutputs();
    int nOutputs = outputs.size();

    // Set the initial positions of all outputs.
    Iterator outputIter = outputs.iterator();
    while (outputIter.hasNext())
      ((Cursor)outputIter.next()).setPosition(1);

    // The last output is fastest-varying; it represents columns.
    // The next to last output represents rows.
    // All other outputs are on the page.
    Cursor colCursor = (Cursor)outputs.get(nOutputs - 1);
    Cursor rowCursor = (Cursor)outputs.get(nOutputs - 2);
    ArrayList pageCursors = new ArrayList();
    for (int i = 0; i < nOutputs - 2; i++)
    {
      pageCursors.add(outputs.get(i));
    }

    // Get the base ValueCursor of the CompoundCursor.
    ValueCursor dataCursor = rootCursor.getValueCursor();

    // Print the pages of the crosstab.
    printPages(pageCursors, 0, rowCursor, colCursor, dataCursor);
  }

  /**
   * Prints the pages of a crosstab.
   *
   * @param pageCursors A List of Cursor objects with values that are pages
   *                    of the crosstab.
   * @param pageIndex An integer that indicates the page.
   * @param rowCursor The Cursor for the rows.
   * @param colCursor The Cursor for the columns.
   * @param dataCursor The ValueCursor that contains the values of the
   *                   root CompoundCursor.
   */
  private void printPages(List pageCursors, int pageIndex, Cursor rowCursor,
                          Cursor colCursor, ValueCursor dataCursor)
  {
    // Get a Cursor for this page.
    Cursor pageCursor = (Cursor)pageCursors.get(pageIndex);

    // Loop over the values of this page dimension
    do
    {
      // If this is the fastest-varying page dimension, print a page.
      if (pageIndex == pageCursors.size() - 1)
      {
        // Print the values of the page dimensions.
        printPageHeadings(pageCursors);

        // Print the column headings.
        printColumnHeadings(colCursor);

        // Print the rows
        printRows(rowCursor, colCursor, dataCursor);

        // Print a couple of blank lines to delimit pages.
        println();
        println();
      }

      // If this is not the fastest-varying page, recurse to the
      // next fastest varying dimension.
      else
      {
        printPages(pageCursors, pageIndex + 1, rowCursor, colCursor,
                   dataCursor);
      }
    }
    while (pageCursor.next());

    // Reset this page dimension Cursor to its first element.
    pageCursor.setPosition(1);
  }

  /**
   * Prints the values of the page dimensions on each page of a crosstab.
   *
   * @param pageCursors A List of Cursor objects for the pages of a crosstab.
   */
  private void printPageHeadings(List pageCursors)
  {
    Iterator pageIter = pageCursors.iterator();
    while (pageIter.hasNext())
    {
      Cursor cursor = (Cursor)pageIter.next();
      if (cursor instanceof CompoundCursor)
      {
        CompoundCursor cc = (CompoundCursor)cursor;
        List outputs = cc.getOutputs();
        ValueCursor output = (ValueCursor)outputs.get(0);
        String value = output.getCurrentString();
        println(value);
      }
      else
      {
        ValueCursor vc = (ValueCursor)cursor;
        String value = vc.getCurrentString();
        println(getLocalValue(value));
      }
    }
    println();
  }

  /**
   * Prints the column headings on each page of a crosstab.
   *
   * @param colCursor A Cursor for the Dimension objects that specify the
   *                  values in the column headings of a crosstab.
   */
  private void printColumnHeadings(Cursor colCursor)
  {
    do
    {
      print("\t");
      if (colCursor instanceof CompoundCursor)
      {
        CompoundCursor cc = (CompoundCursor)colCursor;
        List outputs = cc.getOutputs();
        ValueCursor output = (ValueCursor)outputs.get(0);
        String value = output.getCurrentString();
        print(value);
      }
      else
      {
        ValueCursor vc = (ValueCursor)colCursor;
        String value = vc.getCurrentString();
        print(getLocalValue(value));
      }
    }
    while (colCursor.next());
    println();
    colCursor.setPosition(1);
  }

  /**
   * Prints the rows of each page of a crosstab.
   *
   * @param rowCursor The Cursor for the rows.
   * @param colCursor The Cursor for the columns.
   * @param dataCursor The ValueCursor that contains the values of the
   *                   root CompoundCursor.
   */
  private void printRows(Cursor rowCursor, Cursor colCursor,
                         ValueCursor dataCursor)
  {
    // Loop over rows
    do
    {
      // Print row dimension value
      if (rowCursor instanceof CompoundCursor)
      {
        CompoundCursor cc = (CompoundCursor)rowCursor;
        List outputs = cc.getOutputs();
        ValueCursor output = (ValueCursor)outputs.get(0);
        String value = output.getCurrentString();
        print(value);
      }
      else
      {
        ValueCursor vc = (ValueCursor)rowCursor;
        String value = vc.getCurrentString();
        print(getLocalValue(value));
      }
      print("\t");
      // Loop over columns
      do
      {
        // Print data value
        try
        {
          if (!dataCursor.hasCurrentValue())
            print("NULL");
          else
            print(Math.round(dataCursor.getCurrentDouble()));
        }
        catch (IncorrectDataTypeException e)
        {
          print(dataCursor.getCurrentValue());
        }
        print("\t");
      }
      while (colCursor.next());
      println();

      // Reset the column Cursor to its first element
      colCursor.setPosition(1);
    }
    while (rowCursor.next());

    // Reset the row Cursor to its first element
    rowCursor.setPosition(1);
  }

  /**
   * Gets the local value component of a unique value of a Dimension member.
   * This method expects that the value separation string for the
   * Dimension is double colon characters.
   *
   * @param uniqueValue A String that contains the unique value.
   *
   * @return A String that contains the local value.
   */
  protected String getLocalValue(String uniqueValue)
  {
    int index = 0;
    if (uniqueValue.indexOf("::") > 0)
    {
      index = uniqueValue.lastIndexOf("::");
      String localValue =
        uniqueValue.substring((uniqueValue.lastIndexOf("::") + 2),
                              uniqueValue.length());
      return (localValue);
    }
    else
      return uniqueValue;
  }

  /**
   * An inner class that models an uncloseable PrintWriter for use with
   * System.out.
   */
  private static class UncloseablePrintWriter extends CursorPrintWriter
  {
    public UncloseablePrintWriter(OutputStream out)
    {
      super(new PrintWriter(new BufferedWriter(new OutputStreamWriter(out)),
                            true));
    }

    public void close()
    {
    } // Ignore.
  }
}

