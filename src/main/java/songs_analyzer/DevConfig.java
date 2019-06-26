package songs_analyzer;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author Evgeny Borisov
 */
@Configuration
@Profile("DEV")
public class DevConfig {
    @Bean
    public SparkConf sparkConf(){
        return new SparkConf().setAppName("songs_analyzer").setMaster("local[*]");
    }
}
