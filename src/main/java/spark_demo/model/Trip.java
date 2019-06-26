package spark_demo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Trip implements Serializable {
    private String id;
    private String city;
    private int km;

    public Trip(String line){
        String[] arr = line.split(" ");
        id = arr[0];
        city = arr[1].toLowerCase();
        km = Integer.parseInt(arr[2]);

    }
}
