/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.exercise3;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author gdaniello
 */
public class MapperPM10 extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type{
    private final static Double PM10Threshold = new Double(50);
    private final static IntWritable one = new IntWritable(1);
	
    protected void map(
            Text key,   	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Extract sensor and date from the key
            String[] fields = key.toString().split(",");
                        
            String sensor_id=fields[0];
            Double PM10Level=new Double(value.toString());
            
            // Compare the value of PM10 with the threshold value
            if (PM10Level.compareTo(PM10Threshold)>0)
            {
                // emit the pair (sensor_id, 1)
                context.write(new Text(sensor_id), one);
            }
    }
}
