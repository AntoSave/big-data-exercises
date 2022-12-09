/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework6;

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
public class ReducerFriends extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    public void reduce(NullWritable nop, Iterable<Text> friendList, Context context) throws IOException, InterruptedException {
        String acc = "";
        for (Text friend: friendList) {
            acc += friend.toString() + " ";
        }
        context.write(NullWritable.get(), new Text(acc));
    }

}
