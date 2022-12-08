/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework1v2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * @author alangella
 * 
 * map: (k1, v1) -> [(k2,v2)]
 * Mapper <k1, v1, k2, v2>
 */
  public class MapperBigData extends Mapper<Text, DoubleWritable, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private String sensorID;
    private float threshold;

    public void map(Text key, DoubleWritable sensorValue, Context context) throws IOException, InterruptedException {
      threshold = context.getConfiguration().getFloat("threshold",0.0f);
      sensorID = key.toString().split(",")[0];

      if(sensorValue.get() < threshold){
        return;
      }

      context.write(new Text(sensorID), one);
    }
  }

