package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.util.ToolRunner;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.wordcount.task1.Task1Runner;
import cz.cvut.bigdata.wordcount.task2.Task2Runner;

/**
 * WordCount Example, version 1.0
 * 
 * This is a very simple extension of basic WordCount Example implemented using
 * a new MapReduce API.
 */
public class MainRunner {
	/**
	 * The main entry of the application.
	 */
	public static void main(String[] arguments) throws Exception {
		int status = 0;
		
		final ArgumentParser parser = new ArgumentParser("WordCount");

		parser.addArgument("input", true, true, "specify input directory");
		parser.addArgument("output", true, true, "specify output directory");

		parser.parseAndCheck(arguments);

		final String inputPath = parser.getString("input");
		final String outputDir = parser.getString("output");
		
		status = ToolRunner.run(new Task1Runner(), new String[]{inputPath, outputDir+"1"});
		exitIfFailed(status, "Task 1 failed. Exit code: %d.");
		
		//status = ToolRunner.run(new Task2Runner(), new String[]{outputDir+"1/part-r-00000", outputDir+"2"});
		//exitIfFailed(status, "Task 2 failed. Exit code: %d.");
	}
	
	private static void exitIfFailed(int status, String errorFormat) {
		if(status != 0) {
			System.err.printf(errorFormat+"\n", status);
			System.exit(status);
		}
	}
	
}
