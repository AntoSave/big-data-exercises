/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework1;

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
  public class MapperBigData extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text sensorID = new Text();
    private String row, sensorValue;
    private StringTokenizer t1, t2;
    private float threshold;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      threshold = context.getConfiguration().getFloat("threshold",0.0f);
      row = value.toString();
      t2 = new StringTokenizer(row, "\t");
      t2.nextToken();
      sensorValue=t2.nextToken();

      if(Double.parseDouble(sensorValue) < threshold){
        return;
      }

      t1 = new StringTokenizer(row, ",");
      sensorID.set(t1.nextToken());
      context.write(sensorID, one);
    }
  }

