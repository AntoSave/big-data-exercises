/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework4;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

/**
 *
 * @author alangella
 * 
 *         reduce: (k2, [v2]) -> [(k3,v3)]
 *         Reducer<k2,v2,k3,v3>
 * 
 */
public class ReducerInvertedIndex extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text word, Iterable<Text> sentenceList, Context context) throws IOException, InterruptedException {
        Text result = new Text();
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> set = new HashSet<>();
        //Remove duplicates
        for (Text sentenceID: sentenceList){
            set.add(sentenceID.toString());
        }
        for (String sentenceID: set) {
            stringBuilder.append(sentenceID + " ");
        }
        result.set(stringBuilder.toString());
        context.write(word, result);
    }
}
