package cz.cvut.bigdata.wordcount;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.wordcount.task1.IdfTask1Runner;
import cz.cvut.bigdata.wordcount.task1.Task1Runner;
import cz.cvut.bigdata.wordcount.task3.Task3Runner;
import cz.cvut.bigdata.wordcount.task4.Task4Runner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount Example, version 1.0
 * 
 * This is a very simple extension of basic WordCount Example implemented using
 * a new MapReduce API.
 */
public class MainRunner {

    public static final int TYPO_FREQUENCY = 20;
    public static final int STOP_FREQUENCY = 300_000;
    public static final int UNIQUE_WORD_N = 202_595;
    public static final int TOTAL_WORD_N = 1_266_786_305;
    public static final int DOC_N = 2_322_105;
    public static final double AVG_DOC_LEN = TOTAL_WORD_N/DOC_N;

	/**
	 * The main entry of the application.
	 */
	public static void main(String[] arguments) throws Exception {
		final ArgumentParser parser = new ArgumentParser("WordCount");

        parser.addArgument("query", true, false, "specify query");
		parser.addArgument("input", true, true, "specify input directory");
		parser.addArgument("output", true, true, "specify output directory");

		parser.parseAndCheck(arguments);

        final String query = parser.getString("query");
        final String inputPath = parser.getString("input");
        final String outputDir = parser.getString("output");

        if(query != null) {
            //runTask("Query", new QueryRunner(query), new String[]{outputDir+"4/part-r-00000", outputDir+"1idf/part-r-00000"});
        } else {
            runTask("DF", new Task1Runner(), new String[]{inputPath, outputDir+"1"});
            runTask("IDF", new IdfTask1Runner(), new String[]{inputPath, outputDir+"1idf"});
            runTask("TF", new Task3Runner(), new String[]{inputPath, outputDir+"1/part-r-00000", outputDir+"3"});
            runTask("INV INDEX", new Task4Runner(), new String[]{inputPath, outputDir+"1/part-r-00000", outputDir+"4"});
        }

        //runTask("STOPWORDS and TYPOS", new Task2Runner(), new String[]{outputDir+"1/part-r-00000", outputDir+"2"});
	}

    private static void runTask(String taskName, Tool task, String[] arguments) throws Exception {
        System.out.println("Running "+taskName);
        int status = ToolRunner.run(task, arguments);
        exitIfFailed(status, taskName+" failed. Exit code: %d.");
    }
	
	private static void exitIfFailed(int status, String errorFormat) {
		if(status != 0) {
			System.err.printf(errorFormat+"\n", status);
			System.exit(status);
		}
	}
	
}
