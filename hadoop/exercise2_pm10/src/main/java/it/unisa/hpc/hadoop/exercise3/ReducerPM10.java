/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author gdaniello
 */
public class ReducerPM10 extends
		Reducer<Text, // Input key type
				IntWritable, // Input value type
				Text, // Output key type
				IntWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numDays = 0;

		// Iterate over the set of values and sum them
		for (IntWritable value : values) {
			numDays = numDays + value.get();
		}

		context.write(new Text(key), new IntWritable(numDays));
	}
    
}
