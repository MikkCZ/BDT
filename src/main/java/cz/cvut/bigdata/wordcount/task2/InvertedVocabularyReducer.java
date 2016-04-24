package cz.cvut.bigdata.wordcount.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedVocabularyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable count, Iterable<Text> words, Context context) throws IOException, InterruptedException {
		for (Text word : words) {
			context.write(count, word);
		}
	}
}
