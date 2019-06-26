package taxi;

import org.springframework.stereotype.Service;
import taxi.model.Trip;

/**
 * @author Evgeny Borisov
 */
@Service("boston")
public class BostonPriceCalculator implements PriceCalculator {

    @Override
    public Trip updateTripWithPrice(Trip trip) {
        return trip.withPrice(100);
    }
}
