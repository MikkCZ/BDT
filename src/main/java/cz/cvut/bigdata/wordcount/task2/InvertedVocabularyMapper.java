package cz.cvut.bigdata.wordcount.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedVocabularyMapper extends Mapper<Object, Text, IntWritable, Text> {
	private IntWritable count = new IntWritable();
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] words = value.toString().split("\\s+");
		word.set(words[0]);
		count.set(Integer.parseInt(words[1]));
		context.write(count, word);
	}

}
