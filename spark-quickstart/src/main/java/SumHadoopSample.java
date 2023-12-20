import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


class SumHadoopAggregate implements Serializable {
    public SumHadoopAggregate(Double s){
        Sum = s;
    }
    public Double Sum = 0.0;
}


public class SumHadoopSample {
    private static Iterator<String> ParseNumbers(String numbers){
        String[] dividedNumbers = numbers.split(" ");
        return Arrays.asList(dividedNumbers).iterator();
    }

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("sum-hadoop")
                .set("fs.defaultFS", "hdfs://localhost:8020/");
        SparkContext ssc = SparkContext.getOrCreate(conf);

        // spark uses Scala by default,
        // we need to create a JavaSparkContext to use it with Java
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(ssc);

        // load data from hadoop
        // you will need to publish the file to hadoop first!
        // see `How can I put a file with some test values?` inside `./hadoop-quickstart`
        JavaRDD<String> inputs = sc.textFile("hdfs://localhost:8020/data.txt");

        // convert the string with spaces to an array/iterator
        JavaRDD<String> stringNumbers = inputs.flatMap(SumHadoopSample::ParseNumbers);

        // convert each from string to double
        JavaRDD<Double> numbers = stringNumbers.map(Double::parseDouble);

        // aggregate the data
        SumAggregate aggregate = numbers.aggregate(
                // starting/zero value for each worker
                new SumAggregate(0.0),

                // called to aggregate all data within a worker
                (result, newValue) -> new SumAggregate(result.Sum+newValue),

                // called to aggregate the results of each worker
                (a, b) -> new SumAggregate(a.Sum+b.Sum)
        );

        // aggregate materializes the result
        // materialize = compute and return a value we can use
        System.out.println(">>> The sum is: " + aggregate.Sum + " <<<");
    }
}
