package cz.cvut.bigdata.wordcount.task4;

import cz.cvut.bigdata.wordcount.task1.WordCountPreprocessingMapper;
import cz.cvut.bigdata.wordcount.task3.TfMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InvertMapper extends Mapper<Object, Text, Text, Text> {

    private final Set<String> dfSet;

    public InvertMapper() {
        this.dfSet = new HashSet<>(250_000);
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        URI uri = context.getCacheFiles()[0];
        try(BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath()).getName()))) {
            String line;
            while ((line=br.readLine()) != null) {
                this.dfSet.add(line.split("\\s+")[0]);
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        final String docId = words[0];
        Map<String, Integer> tfVocabulary = new HashMap<>(words.length);

        for(String word : words) {
            String term = WordCountPreprocessingMapper.stripAccents(word).toLowerCase();
            if(dfSet.contains(term) && !WordCountPreprocessingMapper.isN(term)) {
                if(tfVocabulary.containsKey(term)) {
                    tfVocabulary.put(term, tfVocabulary.get(term)+1);
                } else {
                    tfVocabulary.put(term, 1);
                }
            }
        }

        for(Map.Entry<String, Integer> entry : tfVocabulary.entrySet()) {
            context.write(new Text(entry.getKey().toString()), new Text(docId+TfMapper.WORD_COUNT_DELIMITER+entry.getValue().toString()));
        }
    }
}