package it.unisa.hpc.hadoop.homework4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import kotlin.Pair;

/**
 *
 * @author alangella
 */
public class DriverTopK {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Path inputPath;
        Path outputPath;
        int numberOfReducers = 1;

        if(args.length != 3){
            System.err.println("Invalid arguments.\nUsage:\thadoop jar [jarfile] [inputFile] [outputDir] [k]");
            return;
        }

        // Parse the parameters
        inputPath = new Path(args[0]);
        outputPath = new Path(args[1]);
        Configuration conf = new Configuration();
        conf.setInt("k",Integer.parseInt(args[2]));
        // Define a new job
        Job job = Job.getInstance(conf, "inverted index");

        PairWritable.aClass = Text.class;
        PairWritable.bClass = DoubleWritable.class;

        // Set path of the input file and output folder for the job
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Set input and output format. The default behaviour of KeyValueTextInputFormat is good.
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the driver, mapper and reducer classes
        job.setJarByClass(DriverTopK.class);
        job.setMapperClass(MapperTopK.class);
        job.setReducerClass(ReducerTopK.class);
        //job.setCombinerClass(ReducerTopK.class);
        
        // Set mapper output key and value classes
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PairWritable.class);

        // Set reducer output key and value classes
        job.setOutputKeyClass(Text.class); //The string representing the date
        job.setOutputValueClass(DoubleWritable.class); //The income
        
        // Set the number of reducers
        job.setNumReduceTasks(numberOfReducers);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
