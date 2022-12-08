package it.unisa.hpc.hadoop.homework1v2;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.security.auth.callback.TextOutputCallback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author alangella
 */
public class DriverBigData {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Path inputPath;
        Path outputPath;
        Float threshold;

        // Parse the parameters
        threshold = Float.parseFloat(args[0]);
        inputPath = new Path(args[1]);
        outputPath = new Path(args[2]);
        
        Configuration conf = new Configuration();
        conf.setFloat("threshold",threshold);
        //conf.set("key.value.separator.in.input.line", "\t"); //di default è qualunque spazio quindi qui non è necessario

        // Define a new job
        Job job = Job.getInstance(conf, "pm10 count");
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
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); //Di default è questo

        KeyValueTextInputFormat.addInputPath(job, inputPath);

        //FileInputFormat.addInputPath(job, inputPath);
         // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.setNumReduceTasks(1); //posso decidere il numero di reducer

        // Execute the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
