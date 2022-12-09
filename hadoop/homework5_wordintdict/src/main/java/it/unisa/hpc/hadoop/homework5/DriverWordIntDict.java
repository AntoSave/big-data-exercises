package it.unisa.hpc.hadoop.homework5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import kotlin.Pair;

/**
 *
 * @author alangella
 */
public class DriverWordIntDict {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Path inputPath;
        Path outputPath;
        int numberOfReducers = 1;

        if(args.length != 2){
            System.err.println("Invalid arguments.\nUsage:\thadoop jar [jarfile] [inputFile] [outputDir]");
            return;
        }

        // Parse the parameters
        inputPath = new Path(args[0]);
        outputPath = new Path(args[1]);
        Configuration conf = new Configuration();
        
        // Define a new job
        Job job = Job.getInstance(conf, "wordIntDict");

        // Set path of the input file and output folder for the job
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Set input and output format. The default behaviour of KeyValueTextInputFormat is good.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the driver, mapper and reducer classes
        job.setJarByClass(DriverWordIntDict.class);
        job.setMapperClass(MapperWordIntDict.class);
        job.setReducerClass(ReducerWordIntDict.class);
        //job.setCombinerClass(ReducerTopK.class);
        
        // Set mapper output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Set reducer output key and value classes
        job.setOutputKeyClass(Text.class); //The string representing the date
        job.setOutputValueClass(NullWritable.class); //The income
        
        // Set the number of reducers
        job.setNumReduceTasks(numberOfReducers);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
