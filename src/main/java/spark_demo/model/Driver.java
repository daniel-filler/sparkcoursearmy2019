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
public class Driver implements Serializable {
    private String id;
    private String name;

    public Driver(String line){
        String[] arr = line.split(", ");
        id = arr[0];
        name = arr[1];
    }
}
