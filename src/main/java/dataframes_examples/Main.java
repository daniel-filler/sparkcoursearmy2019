package dataframes_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

//        dataFrame.filter(col("age").leq(40))
//        dataFrame.filter(array_contains(col("keywords"),"spring"))
        //dataFrame = dataFrame.withColumn("2", size(col("keywords"))).select("2");
//        dataFrame = dataFrame.withColumn("2", explode(col("keywords"))).select("2");
        dataFrame.show();

        StructType schema = dataFrame.schema();
        System.out.println("schema = " + schema);

        StructField[] fields = schema.fields();
        for (StructField field : fields) {
            System.out.println(field.dataType());
        }
        dataFrame = dataFrame.withColumn("salary", col("age").multiply(10).multiply(size(col("keywords"))));

        dataFrame.withColumn("technology", explode(col("keywords"))).select("technology").groupBy("technology").count().sort(col("count").desc()).take(1);



//        dataFrame.show();
//        dataFrame.withColumn("number of technologies")







//
//        Row[] rows = dataFrame.take(5);
//        for (Row row : rows) {
//            String name = row.getAs("name");
//            System.out.println("name = " + name);
//        }


    }
}
