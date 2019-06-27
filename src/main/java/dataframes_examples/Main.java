package dataframes_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import java.util.Arrays;
import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");

        dataFrame.printSchema();

        System.out.println(Arrays.toString(dataFrame.dtypes()));
        dataFrame = dataFrame
                .withColumn("salary",
                        col("age")
                        .multiply(size(col("keywords")))
                        .multiply(when(col("age").leq(30), 5).otherwise(10)));

        dataFrame.show();

        String topTechnology = dataFrame
                .withColumn("technology", explode(col("keywords")))
                .select("technology")
                .groupBy("technology")
                .count()
                .sort(col("count")
                        .desc())
                .head()
                .getAs("technology");

        dataFrame
                .filter(col("salary")
                        .leq(1200))
                .filter(array_contains(col("keywords"), topTechnology))
                .show();


    }
}
