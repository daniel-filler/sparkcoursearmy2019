package taxi.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;

import java.io.Serializable;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Wither
public class Trip implements Serializable {
    private String id;
    private String city;
    private int km;
    private int price;

    public Trip(String line) {
        String[] arr = line.split(" ");
        id = arr[0];
        city = arr[1].toLowerCase();
        km = Integer.parseInt(arr[2]);
    }


}













