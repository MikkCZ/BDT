package cz.cvut.bigdata.wordcount.task4;

import cz.cvut.bigdata.wordcount.task3.TfMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        for (Text value : values) {
            sb.append(value.toString())
                    .append(TfMapper.WORDS_DELIMITER);
        }

        String tmp = sb.toString();
        context.write(text, new Text(tmp.substring(0, tmp.length()-1)));
    }
}
