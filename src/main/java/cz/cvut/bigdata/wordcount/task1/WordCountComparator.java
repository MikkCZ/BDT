package cz.cvut.bigdata.wordcount.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts word keys in reversed lexicographical order.
 */
public class WordCountComparator extends WritableComparator {
	protected WordCountComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// Here we use exploit the implementation of compareTo(...) in
		// Text.class.
		return -a.compareTo(b);
	}
}