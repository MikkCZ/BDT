package cz.cvut.bigdata.wordcount.task4;

import cz.cvut.bigdata.wordcount.MainRunner;
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

    private static final double K1 = 1.5;
    private static final double B = 0.75;

    private final Set<String> dfSet;
    private final Map<String, Integer> dLen;

    public InvertMapper() {
        this.dfSet = new HashSet<>(250_000);
        this.dLen = new HashMap<>(MainRunner.DOC_N);
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println(context.getCacheFiles()[0].getPath());
        System.out.println(context.getCacheFiles()[1].getPath());
        URI uri1 = context.getCacheFiles()[0]; // "STANKMIC_task1/part-r-00000"
        try(BufferedReader br = new BufferedReader(new FileReader(new File(uri1.getPath()).getName()))) {
            String line;
            while ((line=br.readLine()) != null) {
                String[] split = line.split("\\s+");
                System.out.println(line);
                System.out.println(split[0]);
                this.dfSet.add(split[0]);
            }
        }

        URI uri3 = context.getCacheFiles()[1];
        try(BufferedReader br = new BufferedReader(new FileReader(new File(uri3.getPath()).getName()))) {
            String line;
            while ((line=br.readLine()) != null) {
                String[] split = line.split("\\s+");
                this.dLen.put(split[0], Integer.parseInt(split[1]));
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
                System.out.println(term);
                if(tfVocabulary.containsKey(term)) {
                    tfVocabulary.put(term, tfVocabulary.get(term)+1);
                } else {
                    tfVocabulary.put(term, 1);
                }
            }
        }

        for(Map.Entry<String, Integer> entry : tfVocabulary.entrySet()) {
            //context.write(new Text(entry.getKey().toString()), new Text(docId+TfMapper.WORD_COUNT_DELIMITER+entry.getValue().toString()));
            double f_q_D = entry.getValue();
            double a = f_q_D*(K1+1);
            double b = f_q_D+(K1 * ( 1-B + B*(((double)dLen.get(docId))/MainRunner.AVG_DOC_LEN) ) );
            context.write(new Text(entry.getKey().toString()), new Text(docId+TfMapper.WORD_COUNT_DELIMITER+String.format("%.5f", (a/b))));
        }
    }
}