package cz.cvut.bigdata.wordcount.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	public void reduce(IntWritable count, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		context.write(count, new IntWritable(sum));
	}
}
