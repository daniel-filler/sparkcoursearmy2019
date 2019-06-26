package taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
public class SpringTaxiConfig {

    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("taxi"));
    }

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringTaxiConfig.class);
        context.getBean(PriceHandler.class).printTripsWithPrice();
    }

}










