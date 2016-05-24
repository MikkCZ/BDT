package cz.cvut.bigdata.wordcount.task1;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Receives (byteOffsetOfLine, textOfLine), note we do not care about the
 * type of the key because we do not use it anyway, and emits (word, 1) for
 * each occurrence of the word in the line of text (i.e. the received
 * value).
 */
public class WordCountPreprocessingMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final Text DOC_N_PLACEHOLDER = new Text("_");
    public static final Text WORD_N_PLACEHOLDER = new Text("__");

	private final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");

		for (String term : words) {
			term = stripAccents(term);
			if (isAllowed(term)) {
				word.set(term.toLowerCase());
				context.write(word, ONE);
                //context.write(WORD_N_PLACEHOLDER, ONE); // to count the total number of words (non-unique)
			}
		}

        //context.write(DOC_N_PLACEHOLDER, ONE); // to count the total number of documents
	}

    public static boolean isN(String term) {
        return term.equals(DOC_N_PLACEHOLDER.toString()) || term.equals(WORD_N_PLACEHOLDER.toString());
    }

    public static String stripAccents(String term) {
        return StringUtils.stripAccents(term);
    }

	protected static boolean isAllowed(String term) {
		return term.length() >= 3 &&
				term.length() <= 24 &&
				StringUtils.isAsciiPrintable(term) &&
				StringUtils.isAlpha(term) &&
				uniqueChars(term) >= 2;
	}

    protected static int uniqueChars(String term) {
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