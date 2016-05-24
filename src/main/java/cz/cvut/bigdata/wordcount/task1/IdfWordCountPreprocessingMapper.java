package cz.cvut.bigdata.wordcount.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Receives (byteOffsetOfLine, textOfLine), note we do not care about the
 * type of the key because we do not use it anyway, and emits (word, 1) for
 * each occurrence of the word in the line of text (i.e. the received
 * value).
 */
public class IdfWordCountPreprocessingMapper extends Mapper<Object, Text, Text, Text> {

	private final Text ONE = new Text("1");
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");

		for (String term : words) {
			term = WordCountPreprocessingMapper.stripAccents(term);
			if (WordCountPreprocessingMapper.isAllowed(term)) {
				word.set(term.toLowerCase());
				context.write(word, ONE);
			}
		}
	}
}