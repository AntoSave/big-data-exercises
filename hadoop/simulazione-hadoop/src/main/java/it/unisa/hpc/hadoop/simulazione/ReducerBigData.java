package it.unisa.hpc.hadoop.simulazione;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends
		Reducer<Text, IntWritable, Text, DoubleWritable> { // Output value type


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        int n=0;
        for(IntWritable value: values){
            n++;
            sum += value.get();
        }
        context.write(key, new DoubleWritable(sum/n));
        
    }
    
}
