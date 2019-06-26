package songs_analyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
@PropertySource("classpath:user.properties")
public class Conf {

    @Autowired
    private SparkConf sparkConf;

    @Autowired
    private UserConf userConf;


    @Bean
    public Broadcast<UserConf> userConfBroadcasted(){
        Broadcast<UserConf> broadcast = sc().broadcast(userConf);
        return broadcast;
    }

    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(sparkConf);
    }


    public static void main(String[] args) {

        System.setProperty("spring.profiles.active", "DEV");

        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(Conf.class);
        SongsAnalyzer songsAnalyzer = context.getBean(SongsAnalyzer.class);
        songsAnalyzer.printMostPopularWords("britney", 3);
    }


}
