/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package it.unisa.diem.hpc.spark.exercise6;

import java.util.List;

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
        String outputPath = args[1];
        // Create a configuration object and set the name of the application 
        SparkConf conf = new SparkConf().setAppName("Spark Ex4");
        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Build an RDD of Strings from the input textual file 
        // Each element of the RDD is a line of the input file 
        JavaRDD<String> lines = sc.textFile(inputFile);
        JavaRDD<Double> values = lines.map(line -> line.split(",")[2]).map(Double::parseDouble);
        long count = values.count();
        Double sum = values.reduce((a,b) -> a+b);
        System.out.println(sum/count);
        
        //Close the Spark Context
        sc.close();
    }
        
    
}
