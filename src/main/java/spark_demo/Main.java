package spark_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import spark_demo.model.Driver;
import spark_demo.model.Trip;

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
        JavaRDD<String> drivers = sc.textFile("data/drivers.txt");
        JavaRDD<String> rdd2 = rdd.map(String::toLowerCase);

        //JavaRDD<String> bostonTrips = rdd2.filter(s -> s.contains("boston"));

        long rowCount = rdd.count();
        System.out.println("rowCount = " + rowCount);

        JavaRDD<Trip> tripRdd = rdd.map(Trip::new);
        tripRdd.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Trip> bostonTripsRdd = tripRdd
                .filter(trip -> trip.getCity().equals("boston"));
        bostonTripsRdd.persist(StorageLevel.MEMORY_AND_DISK());
        long boston10kmTripsCount = bostonTripsRdd.filter(trip -> trip.getKm() >= 10).count();
        System.out.println("boston10kmTripsCount = " + boston10kmTripsCount);

        Double totalDistanceOfBostonTrips = bostonTripsRdd
                .mapToDouble(Trip::getKm)
                .sum();
        System.out.println("totalDistanceOfBostonTrips = " + totalDistanceOfBostonTrips);

        JavaPairRDD<String, Integer> driverTotalDistanceRdd = tripRdd
                .mapToPair(trip -> new Tuple2<>(trip.getId(), trip.getKm()))
                .reduceByKey(Integer::sum);

        JavaPairRDD<String, String> driverNameRdd = drivers
                .map(Driver::new)
                .mapToPair(driver -> new Tuple2<>(driver.getId(), driver.getName()));

        driverTotalDistanceRdd
                .join(driverNameRdd)
                .mapToPair(Tuple2::_2)
                .sortByKey(false)
                .take(3)
                .forEach(System.out::println);


/*


        long numOf10kBostonTrips = bostonTrips
                .map(s -> s.split(" "))
                .filter(strings -> Integer.parseInt(strings[2]) > 10)
                .count();

        Integer sumOfBostonTrips = bostonTrips.map(s -> s.split(" "))
                .map(strings -> Integer.parseInt(strings[2]))
                .reduce(Integer::sum);

        JavaPairRDD<Integer, String> driversIdAndName = drivers.map(s -> s.split(", "))
                .mapToPair(s -> new Tuple2<>(Integer.parseInt(s[0]), s[1]));

        List<Tuple3> topDrivers = rdd.map(s -> s.split(" "))
                .mapToPair(strings -> new Tuple2<>(Integer.parseInt(strings[0]), Integer.parseInt(strings[2])))
                .reduceByKey(Integer::sum)
                .leftOuterJoin(driversIdAndName)
                .map(t -> new Tuple3(t._1, t._2._1, t._2._2.get()))
                .sortBy(Tuple3::_2, false, 1)
                .take(3);

        System.out.println("number of lines: " + rowCount);
        System.out.println("number of trips to boston over 10km: " + numOf10kBostonTrips);
        System.out.println("sum of all kilometers trips to Boston: "+ sumOfBostonTrips);
        System.out.println("top drivers: " + topDrivers);

*/

    }
}












