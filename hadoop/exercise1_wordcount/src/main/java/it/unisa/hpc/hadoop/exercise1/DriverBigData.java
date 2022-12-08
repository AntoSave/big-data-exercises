package it.unisa.hpc.hadoop.exercise1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author gdaniello
 */
public class DriverBigData {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Path inputPath;
        Path outputPath;
        
        // Parse the parameters
        inputPath = new Path(args[0]);
        outputPath = new Path(args[1]);
        
        Configuration conf = new Configuration();
        
        // Define a new job
        Job job = Job.getInstance(conf, "word count");
        // Specify the class of the Driver for this job
        job.setJarByClass(DriverBigData.class);
         // Set map class
        job.setMapperClass(MapperBigData.class);
         // Set reduce class
        job.setReducerClass(ReducerBigData.class);
        
        // Set map output key and value classes. 
        // If these are the same of the reducer, it is sufficient to set the outputKey and outputValueClass
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(IntWritable.class);
        
        // Set reducer output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job, inputPath);
         // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Execute the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
