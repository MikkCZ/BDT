package cz.cvut.bigdata.wordcount.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class Task2Runner extends Configured implements Tool {
	
	/**
	 * This is where the MapReduce job is configured and being launched.
	 */
	@Override
	public int run(String[] arguments) throws Exception {
		final Path invertInputPath = new Path(arguments[0]);
		final Path invertOutputDir = new Path(arguments[1]+"invert");
		final Path histogramInputPath = new Path(arguments[1]+"invert/part-r-00000");
		final Path histogramOutputDir = new Path(arguments[1]+"histogram");
		
		return (runInvert(invertInputPath, invertOutputDir) && runHistogram(histogramInputPath, histogramOutputDir)) ? 0 : 1;
	}
	
	private boolean runInvert(Path inputPath, Path outputDir) throws Exception {
		// Create configuration.
		Configuration conf = getConf();
//		conf.set("textinputformat.record.delimiter", "\n");

		// Using the following line instead of the previous
		// would result in using the default configuration
		// settings. You would not have a change, for example,
		// to set the number of reduce tasks (to 5 in this
		// example) by specifying: -D mapred.reduce.tasks=5
		// when running the job from the console.
		//
		// Configuration conf = new Configuration(true);

		// Create job.
		Job job = Job.getInstance(conf, "Inverted Vocabulary");
		job.setJarByClass(InvertedVocabularyMapper.class);

		// Setup MapReduce.
		job.setMapperClass(InvertedVocabularyMapper.class);
		job.setReducerClass(InvertedVocabularyReducer.class);

		// Make use of a combiner - in this simple case
		// it is the same as the reducer.
		job.setCombinerClass(InvertedVocabularyReducer.class);

		// Sort the output words in reversed order.
		job.setSortComparatorClass(IntWritableComparator.class);

		// Use custom partitioner.
//		job.setPartitionerClass(WordLengthPartitioner.class);

		// By default, the number of reducers is configured
		// to be 1, similarly you can set up the number of
		// reducers with the following line.
		//
		// job.setNumReduceTasks(1);

		// Specify (key, value).
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// Input.
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output.
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem hdfs = FileSystem.get(conf);

		// Delete output directory (if exists).
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute the job.
		return job.waitForCompletion(true);
	}

	private boolean runHistogram(Path inputPath, Path outputDir) throws Exception {
		// Create configuration.
		Configuration conf = getConf();
//		conf.set("textinputformat.record.delimiter", "\n");

		// Using the following line instead of the previous
		// would result in using the default configuration
		// settings. You would not have a change, for example,
		// to set the number of reduce tasks (to 5 in this
		// example) by specifying: -D mapred.reduce.tasks=5
		// when running the job from the console.
		//
		// Configuration conf = new Configuration(true);

		// Create job.
		Job job = Job.getInstance(conf, "Histogram");
		job.setJarByClass(HistogramMapper.class);

		// Setup MapReduce.
		job.setMapperClass(HistogramMapper.class);
		job.setReducerClass(HistogramReducer.class);

		// Make use of a combiner - in this simple case
		// it is the same as the reducer.
		job.setCombinerClass(HistogramReducer.class);

		// Sort the output words in reversed order.
		job.setSortComparatorClass(IntWritableComparator.class);

		// Use custom partitioner.
//		job.setPartitionerClass(WordLengthPartitioner.class);

		// By default, the number of reducers is configured
		// to be 1, similarly you can set up the number of
		// reducers with the following line.
		//
		// job.setNumReduceTasks(1);

		// Specify (key, value).
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Input.
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output.
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem hdfs = FileSystem.get(conf);

		// Delete output directory (if exists).
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute the job.
		return job.waitForCompletion(true);
	}
}
