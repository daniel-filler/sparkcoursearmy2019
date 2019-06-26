package taxi;

import org.apache.spark.api.java.JavaRDD;
import taxi.model.Trip;

import java.io.Serializable;

/**
 * @author Evgeny Borisov
 */
@FunctionalInterface
public interface PriceCalculator extends Serializable {
    Trip updateTripWithPrice(Trip trip);
}
