package it.unisa.diem.hpc.spark.simulazione;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    /**
     * @param args the command line arguments
     */
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
        JavaRDD<String> lines = sc.textFile(inputFile);
        lines.

        // Close the Spark context
        sc.close();
    }
    
}
