package cz.cvut.bigdata.wordcount.query;

import cz.cvut.bigdata.wordcount.task3.TfMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Query {

    private static final int MAX_RESULTS = 20;

    public Query(String query, String task1idf, String task4) throws IOException {
        final Set<String> querySet = Arrays.asList((query.split("\\s+"))).stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        final Map<String, Double> results = query(
                querySet,
                task1idf,
                task4
        );
        System.out.printf("Found %d documents in total.\n", results.size());
        System.out.printf("Limit set to %d.\n", MAX_RESULTS);
        results.entrySet()
                .stream()
                .sorted((e1, e2) -> -(e1.getValue().compareTo(e2.getValue())))
                .limit(MAX_RESULTS)
                .forEachOrdered(
                        e -> System.out.printf(
                                "Score: %2$.5f, Link: https://en.wikipedia.org/?curid=%1$S\n",
                                e.getKey(),
                                e.getValue()
                        )
                );
    }

    private Map<String, Double> query(Set<String> querySet, String task1idf, String task4) throws IOException {
        final Map<String, Double> result = new HashMap<>();

        final Map<String, Double> idf = loadIdf(task1idf, querySet); // [qi, idf]
        int read = 0;
        try(final BufferedReader br = new BufferedReader(new FileReader(task4))) {
            String line;
            while ((line=br.readLine()) != null) {
                final String[] lineSplit = line.split("\\s+");
                final Double wordIdf = idf.get(lineSplit[0]);
                if (wordIdf != null) {
                    final String[] docsSplit = lineSplit[1].split(TfMapper.WORDS_DELIMITER);
                    for (String doc : docsSplit) {
                        final String[] tmp = doc.split(TfMapper.WORD_COUNT_DELIMITER);
                        final String docId = tmp[0];
                        Double partialResult = result.get(docId);
                        if (partialResult == null) {
                            partialResult = 0d;
                        }
                        result.put(docId, partialResult+(wordIdf*Double.parseDouble(tmp[1])));
                    }
                    read++;
                }
                if(read >= idf.size()) {
                    break;
                }
            }
        }

        return result;
    }

    private Map<String, Double> loadIdf(String idfFile, Set<String> querySet) throws IOException {
        final Map<String, Double> idf = new HashMap<>(querySet.size());
        try(BufferedReader br = new BufferedReader(new FileReader(idfFile))) {
            String line;
            while ((line=br.readLine()) != null) {
                String[] lineSplit = line.split("\\s+");
                if (querySet.contains(lineSplit[0])) {
                    idf.put(lineSplit[0], Double.parseDouble(lineSplit[1]));
                }
                if (idf.size() >= querySet.size()) {
                    break;
                }
            }
        }
        System.out.printf("Loaded %d IDF.\n", idf.size());
        return idf;
    }

}
