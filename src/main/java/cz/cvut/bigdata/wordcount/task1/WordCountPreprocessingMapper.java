package cz.cvut.bigdata.wordcount.task1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Receives (byteOffsetOfLine, textOfLine), note we do not care about the
 * type of the key because we do not use it anyway, and emits (word, 1) for
 * each occurrence of the word in the line of text (i.e. the received
 * value).
 */
public class WordCountPreprocessingMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");

		for (String term : words) {
			term = StringUtils.stripAccents(term);
			if (isAllowed(term)) {
				word.set(term.toLowerCase());
				context.write(word, ONE);
			}
		}
	}

	private boolean isAllowed(String term) {
		return term.length() >= 3 &&
				term.length() <= 24 &&
				StringUtils.isAsciiPrintable(term) &&
				StringUtils.isAlpha(term) &&
				uniqueChars(term) >= 2;
	}
	
	private int uniqueChars(String term) {
		if(term == null || term.isEmpty()) {
			return 0;
		}
		final Set<Character> chars = new HashSet<>(term.length()); 
		for(char c : term.toCharArray()) {
			chars.add(c);
		}
		return chars.size();
	}
}