/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package it.unisa.hpc.spark.exercise2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String inputFile = args[0];
        String outputPath = args[1];
        // Create a configuration object and set the name of the application 
        SparkConf conf = new SparkConf().setAppName("Spark Log Analysis 1");
        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Build an RDD of Strings from the input textual file 
        // Each element of the RDD is a line of the input file 
        JavaRDD<String> lines = sc.textFile(inputFile);
        
        // Only the elements of the RDD satisfying the filter  are included in the
	// google RDD
        JavaRDD<String> google = lines.filter((logLine) -> logLine.toLowerCase().contains("google"));
        
        JavaRDD<String> ipAddresses = google.map((line) -> line.split("\\s+")[0]);
        JavaRDD<String> uniqueAddresses = ipAddresses.distinct();

        //STORE the result in the output folder
        uniqueAddresses.saveAsTextFile(outputPath);
        
        //Close the Spark Context
        sc.close();
    }
        
    
}
