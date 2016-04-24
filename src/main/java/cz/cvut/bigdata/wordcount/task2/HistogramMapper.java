package cz.cvut.bigdata.wordcount.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistogramMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private final IntWritable ONE = new IntWritable(1);
	private IntWritable count = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] words = value.toString().split("\\s+");
		count.set(Integer.parseInt(words[0]));
		context.write(count, ONE);
	}

}
