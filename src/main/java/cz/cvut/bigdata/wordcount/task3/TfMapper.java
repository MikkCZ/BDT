package cz.cvut.bigdata.wordcount.task3;

import cz.cvut.bigdata.wordcount.task1.WordCountPreprocessingMapper;
import org.apache.hadoop.io.IntWritable;
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

public class TfMapper extends Mapper<Object, Text, IntWritable, Text> {

    public static final String WORD_COUNT_DELIMITER = ":";
    public static final String WORDS_DELIMITER = ",";

    private final Set<String> dfSet;

    public TfMapper() {
        this.dfSet = new HashSet<>(250_000);
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        URI uri = context.getCacheFiles()[0];
        try(BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath()).getName()))) {
            String line;
            while ((line=br.readLine()) != null) {
                this.dfSet.add(line.split("\\s+")[0]);
                System.out.println(line.split("\\s+")[0]);
            }
        }/* catch (IOException e) {
            System.err.printf("Cached file uri: %s\n", uri.toString());
            System.err.printf("Cached file path: %s\n", uri.getPath());
            System.err.printf("Current dir: %s\n", new File(".").getCanonicalPath());
            System.err.printf("Current dir using System: %s\n", System.getProperty("user.dir"));
            throw e;
        }*/
        System.out.printf("Words in vocabulary: %d.\n", dfSet.size());
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        final int docId = Integer.parseInt(words[0]);
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

        if(!tfVocabulary.isEmpty()) {
            context.write(new IntWritable(docId), new Text(""+words.length+"\t"+formatOutput(tfVocabulary)));
        }
    }

    private String formatOutput(Map<String, Integer> tfVocabulary) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, Integer> entry : tfVocabulary.entrySet()) {
            sb.append(entry.getKey())
                    .append(WORD_COUNT_DELIMITER)
                    .append(entry.getValue())
                    .append(WORDS_DELIMITER);
        }
        String tmp = sb.toString();
        return tmp.substring(0, tmp.length()-1);
    }
}