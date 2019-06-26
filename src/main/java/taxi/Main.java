package taxi;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import taxi.model.Trip;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        SparkConf conf = sparkConf.setMaster("local[*]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("data\\taxi_orders.txt");
        long count = rdd.count();
        System.out.println("count = " + count);

        JavaRDD<Trip> tripRdd = rdd.map(Trip::new);
        tripRdd.persist(StorageLevel.MEMORY_AND_DISK());

        Accumulator<Integer> smallTripsCountAcc = sc.accumulator(0);

        tripRdd.foreach(trip -> {
            if(trip.getKm()<5){
                smallTripsCountAcc.add(1);
            }
        });

        System.out.println(smallTripsCountAcc.value());

        JavaRDD<Trip> bostonTripsRdd = tripRdd.filter(trip -> trip.getCity().equals("boston"));
        bostonTripsRdd.persist(StorageLevel.MEMORY_AND_DISK());
        long amountOfLongTripsToBoston = bostonTripsRdd.filter(trip -> trip.getKm() >= 10).count();
        System.out.println("amountOfLongTripsToBoston = " + amountOfLongTripsToBoston);


        Double totalToBoston = bostonTripsRdd.mapToDouble(Trip::getKm).sum();

        System.out.println("totalToBoston = " + totalToBoston);

        JavaPairRDD<String, Integer> id2TotalKmRdd = tripRdd
                .mapToPair(trip -> new Tuple2<>(trip.getId(), trip.getKm()))
                .reduceByKey(Integer::sum);


        JavaPairRDD<String, String> driversRdd = sc.textFile("data/drivers.txt")
                .map(line -> line.split(", "))
                .mapToPair(arr -> new Tuple2<>(arr[0], arr[1]));


        id2TotalKmRdd.join(driversRdd)
                .mapToPair(Tuple2::_2)
                .sortByKey(false)
                .take(3)
                .forEach(System.out::println);


    }
}












