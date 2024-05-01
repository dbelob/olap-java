package acme.oracledb;

import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

public abstract class BaseExample {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BaseExample.class);

    public static final String KEY_LOGFILE = "logfile";
    public static final String VALUE_YES = "yes";
    public static final String KEY_VERBOSE = "verbose";
    public static final Properties props = new Properties();

    private Context context;
    protected boolean createContext = true;

    private CursorPrintWriter cpw;
    private Logger logger;

    protected abstract void run() throws Exception;

    public void execute(Properties properties) {
        // Note that we are not handling any exceptions here.
        // This is for the caller to do.
        // However, we make sure the example framework is shut down properly.
        try {
            initialize(properties);
            run();
        } catch (Exception e) {
            printStackTrace(e);
        } finally {
            shutdown();
        }
    }

    protected void initialize(Properties properties) throws Exception {
        setUpCursorPrintWriter(properties);
        setupLogger(getCursorPrintWriter(), properties);

        if (createContext) {
            // Create a Context11g object.
            // println("Creating a Context11g object to make the connection.");
            createContext(properties);
        } else {
            // Do not create a Context11g object.
            // println("Connecting without creating a Context11g object.");
        }
    }

    protected void shutdown() {
        closeContext();
        logger.close();
        logger = null;
        closeCursorPrintWriter();
    }

    protected void createContext(Properties properties) {
        context = new Context(properties, getLogger());
    }

    protected void closeContext() {
        try {
            context.close();
        } catch (Exception e) {
            log.error("Context connection error", e);
        } finally {
            context = null;
        }
    }

    public void execute(String[] args) {
        execute(convertToProperties(args));
    }

    final void setupLogger(PrintWriter writer, Properties properties) {
        logger = (writer == null ? new Logger() : new Logger(writer));
        boolean verbose = VALUE_YES.equalsIgnoreCase(properties.getProperty(KEY_VERBOSE));
        logger.setVerbose(verbose);
    }

    protected final Logger getLogger() {
        return logger;
    }

    final void setUpCursorPrintWriter(Properties properties) throws IOException {
        String logFile = properties.getProperty(KEY_LOGFILE);
        if (null == logFile) {
            cpw = CursorPrintWriter.getStandardOutput(); // default to Stdout
        } else {
            PrintWriter writer = new PrintWriter(new FileWriter(logFile));
            cpw = new CursorPrintWriter(writer);
        }
    }

    protected CursorPrintWriter getCursorPrintWriter() {
        return cpw;
    }

    final void closeCursorPrintWriter() {
        if (cpw != null) {
            cpw.close();
        }

        cpw = null;
    }

    void printStackTrace(Exception e) {
        if (null != cpw) {
            e.printStackTrace(cpw);
        }
    }

    public static Properties convertToProperties(String[] args) {
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 == args.length || !args[i].startsWith("-")) {
                throw new IllegalArgumentException("Command-line arguments must be " +
                        "specified in the form -<property_name> <value> with " +
                        "the property name and value separated by whitespace.");
            }
            props.put(args[i].substring(1), args[i + 1]);
        }

        return props;
    }
}
