package cz.cvut.bigdata.wordcount.task1;

import cz.cvut.bigdata.wordcount.MainRunner;
import org.apache.hadoop.io.IntWritable;
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
public class WordCountVocabularyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text text, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		final boolean isN = WordCountPreprocessingMapper.isN(text.toString());

		for (IntWritable value : values) {
			sum += value.get();
			if(sum > MainRunner.STOP_FREQUENCY && !isN ) { // too many occurrences
				return;
			}
		}
		
		if(sum < MainRunner.TYPO_FREQUENCY && !isN) { // too few occurrences
			return;
		}

		context.write(text, new IntWritable(sum));
	}
}