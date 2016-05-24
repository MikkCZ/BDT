package cz.cvut.bigdata.wordcount.task1;

import cz.cvut.bigdata.wordcount.MainRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Receives (word, list[1, 1, 1, 1, ..., 1]) where the number of 1
 * corresponds to the total number of times the word occurred in the input
 * text, which is precisely the value the reducer emits along with the
 * original word as the key.
 * 
 * NOTE: The received list may not contain only 1s if a combiner is used.
 */
public class IdfWordCountVocabularyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text text, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (Text value : values) {
			sum += Integer.parseInt(value.toString());
			if(sum > MainRunner.STOP_FREQUENCY) { // too many occurrences
				return;
			}
		}
		
		if(sum < MainRunner.TYPO_FREQUENCY) { // too few occurrences
			return;
		}

		context.write(text, new Text(idf(sum)));
	}

	private String idf(int sum) {
		double a = ((double)MainRunner.TOTAL_WORD_N)-sum+0.5;
		double b = ((double)sum)+0.5;
		double idf = Math.log(a/b);
		return String.format("%.5f", idf);
	}
}