package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by michal on 24.5.16.
 */
public class AlwaysFirstPartitioner extends Partitioner<Object, Object> {

    @Override
    public int getPartition(Object key, Object value, int numOfPartitions) {
        return 0;
    }
}