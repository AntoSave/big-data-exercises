/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework4;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author alangella
 * 
 *         reduce: (k2, [v2]) -> [(k3,v3)]
 *         Reducer<k2,v2,k3,v3>
 * 
 */
public class ReducerTopK extends Reducer<NullWritable, PairWritable<Text, DoubleWritable>, Text, DoubleWritable> {
    private List<PairWritable<Text, DoubleWritable>> globalTopK;
    private int k;
    private final Comparator<PairWritable<Text, DoubleWritable>> comparator = new MyComparator();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        globalTopK = new LinkedList<>();
        k = context.getConfiguration().getInt("k", 3);
    }
    
    public void reduce(NullWritable key, Iterable<PairWritable<Text, DoubleWritable>> localTopK, Context context) throws IOException, InterruptedException {
        for(PairWritable<Text, DoubleWritable> pair: localTopK){
            int i;
            // If globalTopK has an element, first element is min. If pair < min pair, ignore pair.
            if(globalTopK.size()>0 && comparator.compare(pair, globalTopK.get(0))<0){
                continue;
            }
            // Insert pair in order
            for(i=0; i < globalTopK.size() && comparator.compare(globalTopK.get(i), pair) <= 0;i++);
            globalTopK.add(i, pair);
            // Remove min if size > k
            if(globalTopK.size()>k)
                globalTopK.remove(0);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        /*ListIterator<PairWritable<Text, DoubleWritable>> iterator = globalTopK.listIterator(globalTopK.size());
        while(iterator.hasPrevious()){
            PairWritable<Text, DoubleWritable> pair = iterator.previous();
            context.write(pair.getFirst(), pair.getSecond());
        }*/
        for(PairWritable<Text, DoubleWritable> pair: globalTopK){
            context.write(pair.getFirst(), pair.getSecond());
        }
        super.cleanup(context);
    }

}
