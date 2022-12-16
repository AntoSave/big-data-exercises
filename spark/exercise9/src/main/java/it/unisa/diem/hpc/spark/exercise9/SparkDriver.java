/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package it.unisa.diem.hpc.spark.exercise9;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String inputFile = args[0];
        //String outputPath = args[1];
        
        // Create a configuration object and set the name of the application 
        SparkConf conf = new SparkConf().setAppName("Spark Ex4");
        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Build an RDD of Strings from the input textual file 
        // Each element of the RDD is a line of the input file 
        JavaRDD<String> lines = sc.textFile(inputFile);
        JavaPairRDD<String, Double> sensorValue = lines.mapToPair(line -> {
            String split[] = line.split(",");
            return new Tuple2<String, Double>(split[0], Double.parseDouble(split[2]));
        });
        JavaPairRDD<String, Double> filteredSensorValue = sensorValue.filter(t -> t._2() >= 50.);
        JavaPairRDD<String, Integer> sensorOnePair = filteredSensorValue.mapValues(v -> 1);
        JavaPairRDD<String, Integer> sensorNumber = sensorOnePair.reduceByKey((a,b)->a+b);
        JavaPairRDD<String, Integer> result = sensorNumber.filter(t -> t._2() >= 2);
        System.out.println(result.collect());
        //Close the Spark Context
        sc.close();
    }
        
    
}
