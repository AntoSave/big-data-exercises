package it.unisa.hpc.hadoop.simulazione;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<Object, Text, Text, IntWritable> {// Output value type
    
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String split[] = row.split(",");
        if(!(split[4].equals("06127") || split[4].equals("06128") || split[4].equals("06227"))){
            return;
        }
        context.write(new Text(split[4]), new IntWritable(Integer.parseInt(split[1])));
    }
    
}
