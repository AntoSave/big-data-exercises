package it.unisa.hpc.hadoop.simulazione;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DriverBigData {
    
    public static void main(String args[]) throws Exception {
        
        Path inputPath;
        Path outputDir;

        // Parse the parameters
        inputPath = new Path(args[0]);
        outputDir = new Path(args[1]);

        Configuration conf = new Configuration();

        // Define a new job
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("Exam");

        // Set path of the input file/folder (if it is a folder, the job reads
        // all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir);

        // Specify the class of the Driver for this job
        job.setJarByClass(DriverBigData.class);

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperBigData.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(ReducerBigData.class);

        // Set reduce (and combiner) output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Double.class);


        //Execute job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
