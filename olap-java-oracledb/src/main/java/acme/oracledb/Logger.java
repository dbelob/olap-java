package acme.oracledb;

import java.io.*;

/**
 * This class encapsulates writing messages to a log.
 *
 * @author mruokone
 */
public class Logger
{
  private PrintWriter writer = null;

  /**
   * Specifies whether a program produces more or less detailed information.
   */
  private boolean verbose = true;

  public Logger()
  {
  }

  public Logger(PrintWriter out)
  {
    setWriter(out);
  }

  public Logger(Writer out)
  {
    this(new PrintWriter(out));
  }

  /**
   * Specifies whether the example program produces more or less information.
   *
   * @return A boolean that is true if the Logger produces more informatiion or
   *        false if it produces less information
   */
  public boolean isVerbose()
  {
    return this.verbose;
  }

  /**
   * Specifies whether the example program produces more or less information.
   *
   * @param verbose To have the Logger produces more information, specify true.
   */
  public void setVerbose(boolean verbose)
  {
    this.verbose = verbose;
  }

  /**
   * Gets the PrintWriter that the Logger is using.
   *
   * @return The PrintWriter for the Logger.
   */
  public PrintWriter getPrintWriter()
  {
    return writer;
  }

  /**
   * Specifies the PrintWriter to use.
   *
   * @param writer The PrintWriter to use.
   */
  public void setWriter(PrintWriter writer)
  {
    close(); // Close the current writer.
    this.writer = writer;
  }

  /**
   * Specifies a buffered writer for output streams.
   *
   * @param os A PrintStream.
   */
  public void setWriter(PrintStream os)
  {
    setWriter(new PrintWriter(new BufferedWriter(new OutputStreamWriter(os))));
  }

  /**
   * Specifies an output device if no writer is set.
   */
  protected void initWriter()
  {
    if (writer == null)
      setWriter(System.out);
  }

  /**
   * Prints a message in a line of output.
   *
   * @param message The message to print.
   */
  public void println(String message)
  {
    initWriter();
    writer.println(message);
  }

  /**
   * Prints a message.
   *
   * @param message The message to print.
   */
  public void print(String message)
  {
    initWriter();
    writer.print(message);
  }

  /**
   * Prints a blank line of output.
   */
  public void println()
  {
    initWriter();
    writer.println();
  }

  /**
   * Prints a message in a line of output.
   * If the verbose property is true, the message contains more
   * detailed information.
   *
   * @param message The message to print.
   */
  public void showInfo(String message)
  {
    if (isVerbose())
    {
      initWriter();
      writer.println(message);
    }
  }

  /**
   * Prints an error message in a line of output.
   *
   * @param message The errormessage to print.
   */
  public void showError(String message)
  {
    initWriter();
    writer.print("ERROR: ");
    writer.println(message);
  }

  /**
   * Flushes any buffered messages and closes the writer.
   */
  public void close()
  {
    if (null != writer)
    {
      writer.flush();
      writer = null;
    }
  }
}
