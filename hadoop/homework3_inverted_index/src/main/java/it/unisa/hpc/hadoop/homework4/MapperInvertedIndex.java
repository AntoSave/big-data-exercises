/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework4;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * @author alangella
 * 
 *         map: (k1, v1) -> [(k2,v2)]
 *         Mapper <k1, v1, k2, v2>
 */
public class MapperInvertedIndex extends Mapper<Text, Text, Text, Text> {
    private String word;
    private StringTokenizer tokenizer;

    public void map(Text sentenceID, Text sentence, Context context) throws IOException, InterruptedException {
        for(String word: sentence.toString().split("\\s+")){
            if(word.equals("and") || word.equals("or") || word.equals("not")){
                continue;
            }
            context.write(new Text(word), sentenceID);
        }
    }
}
