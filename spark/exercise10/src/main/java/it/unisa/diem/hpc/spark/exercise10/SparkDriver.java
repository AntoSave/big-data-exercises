package it.unisa.diem.hpc.spark.exercise10;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {
    public static void main(String[] args) {
        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("Exam Spark");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //TODO: WRITE YOUR SOLUTION HERE
        JavaRDD<String> rows = sc.textFile(inputPath);
        JavaPairRDD<String, Tuple2<LocalDate, LocalDate>> degreeDatesPairs = rows.mapToPair((row) -> {
            String split[] = row.split(",");
            Tuple2<LocalDate, LocalDate> datesTuple = new Tuple2<LocalDate,LocalDate>(LocalDate.parse(split[2]), LocalDate.parse(split[3]));
            return new Tuple2<String,Tuple2<LocalDate,LocalDate>>(split[4], datesTuple);
        });
        //SimpleDateFormat a = new SimpleDateFormat();
        JavaPairRDD<String, Long> degreeDutationPairs = degreeDatesPairs.mapToPair((elem) -> new Tuple2<String, Long>(elem._1(),ChronoUnit.DAYS.between(elem._2()._1(), elem._2()._2())));
        JavaPairRDD<String, Long> result = degreeDutationPairs.reduceByKey(Long::max);
        result.saveAsTextFile(outputPath);
        // Close the Spark context
        sc.close();
    }
        
    
}
