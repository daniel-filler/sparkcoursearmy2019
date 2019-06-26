package taxi;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import taxi.model.Trip;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Evgeny Borisov
 */
@Service
public class PriceHandler implements Serializable {
    @Autowired
    private Map<String, PriceCalculator> calculatorMap;

    @Autowired
    private transient JavaSparkContext sc;

    public void printTripsWithPrice() {
        JavaRDD<String> rdd = sc.textFile("data/taxi_orders.txt");
        JavaRDD<Trip> tripsRdd = rdd.map(Trip::new);
        tripsRdd.map(trip -> {
            PriceCalculator calculator =
                    calculatorMap.getOrDefault(trip.getCity(), trip1 -> trip1.withPrice(666));
            return calculator.updateTripWithPrice(trip);
        }).collect().forEach(System.out::println);

    }


}
