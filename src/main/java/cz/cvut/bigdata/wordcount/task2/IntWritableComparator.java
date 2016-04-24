package cz.cvut.bigdata.wordcount.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntWritableComparator extends WritableComparator {
	protected IntWritableComparator() {
		super(IntWritable.class, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return a.compareTo(b);
	}
}
