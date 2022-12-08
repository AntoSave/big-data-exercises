/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework4;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.io.output.LockableFileWriter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.StringTokenizer;

/**
 *
 * @author alangella
 * 
 *         map: (k1, v1) -> [(k2,v2)]
 *         Mapper <k1, v1, k2, v2>
 */
public class MapperTopK extends Mapper<Text, Text, NullWritable, PairWritable<Text, DoubleWritable>> {
    private LinkedList<PairWritable<Text, DoubleWritable>> localTopK;
    
    private int k;
    private final Comparator<PairWritable<Text, DoubleWritable>> comparator = new MyComparator();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        localTopK = new LinkedList<>();
        k = context.getConfiguration().getInt("k",3);
    }
    
    @Override
    public void map(Text date, Text income_t, Context context) {
        double income = Double.parseDouble(income_t.toString());
        PairWritable<Text, DoubleWritable> pair = new PairWritable<Text,DoubleWritable>(date, new DoubleWritable(income));
        localTopK.add(pair);
        if(localTopK.size()>k){
            PairWritable<Text, DoubleWritable> min = localTopK.stream().min(comparator).get();
            localTopK.remove(min);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(PairWritable<Text, DoubleWritable> pair: localTopK){
            context.write(NullWritable.get(), pair);
        }
        super.cleanup(context);
    }
}
