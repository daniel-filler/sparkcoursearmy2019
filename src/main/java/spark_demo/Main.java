package spark_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        SparkConf conf = sparkConf.setMaster("local[*]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("data/taxi_orders.txt");
        JavaRDD<String> rdd2 = rdd.map(String::toLowerCase);
/*
        JavaRDD<String> rdd3 = rdd2.map(String::toUpperCase);
        rdd3.saveAsTextFile("");
        List<String> list = rdd3.collect();
*/
        JavaRDD<String> bostonTrips = rdd2.filter(s -> s.contains("boston"));

        long rowCount = rdd.count();

        long numOf10kBostonTrips = bostonTrips
                .map(s -> s.split(" "))
                .filter(strings -> Integer.parseInt(strings[2]) > 10)
                .count();

        Integer sumOfBostonTrips = bostonTrips.map(s -> s.split(" "))
                .map(strings -> Integer.parseInt(strings[2]))
                .reduce(Integer::sum);

        List<Tuple2> topDrivers = rdd.map(s -> s.split(" "))
                .mapToPair(strings -> new Tuple2<>(Integer.parseInt(strings[0]), Integer.parseInt(strings[2])))
                .reduceByKey(Integer::sum)
                .map(t -> new Tuple2(t._1, t._2))
                .sortBy(Tuple2::_2, false, 1)
                .take(3);

        System.out.println("number of lines: " + rowCount);
        System.out.println("number of trips to boston over 10km: " + numOf10kBostonTrips);
        System.out.println("sum of all kilometers trips to Boston: "+ sumOfBostonTrips);
        System.out.println("top drivers: " + topDrivers);


    }
}












