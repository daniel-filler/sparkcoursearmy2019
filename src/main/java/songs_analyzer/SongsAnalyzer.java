package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class SongsAnalyzer {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private TopXService topXService;

    public void printMostPopularWords(String artist, int x) {
        JavaRDD<String> rdd = sc.textFile("data/songs/" + artist + "/*");
        List<String> list = topXService.topX(rdd, x);
        System.out.println(list);
    }













}
