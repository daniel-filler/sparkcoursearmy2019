package taxi;

import org.springframework.stereotype.Component;
import taxi.model.Trip;

/**
 * @author Evgeny Borisov
 */
@Component("flemington")
public class FlemingtonPriceCalc implements PriceCalculator {
    @Override
    public Trip updateTripWithPrice(Trip trip) {
        return trip.withPrice(500);
    }
}
