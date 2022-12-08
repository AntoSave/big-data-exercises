/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework5;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang3.ObjectUtils.Null;
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
public class ReducerWordIntDict extends Reducer<Text, NullWritable, Text, NullWritable> {
    private int UID;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        UID=0;
    }
    
    public void reduce(Text word, Iterable<NullWritable> nulls, Context context) throws IOException, InterruptedException {
        context.write(new Text("("+word.toString()+", "+ UID +")"), NullWritable.get());
        UID++;
    }

}
