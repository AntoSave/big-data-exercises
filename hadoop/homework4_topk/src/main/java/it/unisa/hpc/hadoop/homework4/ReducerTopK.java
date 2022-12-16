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
import java.util.TreeMap;
import java.util.TreeSet;

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
public class ReducerTopK extends Reducer<NullWritable, DateIncomeWritable, NullWritable, DateIncomeWritable> {
    private LinkedList<DateIncomeWritable> globalTopK;
    private int k;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        globalTopK = new LinkedList<>();
        k = context.getConfiguration().getInt("k", 3);
    }
    
    public void reduce(NullWritable key, Iterable<DateIncomeWritable> localTopK, Context context) throws IOException, InterruptedException {
        for(DateIncomeWritable pair: localTopK){
            globalTopK.add(pair);
        }
        globalTopK.sort(new MyComparator());
        int i = 0;
        while(i<k){
            context.write(NullWritable.get(), globalTopK.get(globalTopK.size()-i));
            i++;
        }
    }

}
