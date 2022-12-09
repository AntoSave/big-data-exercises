/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.unisa.hpc.hadoop.homework6;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.io.output.LockableFileWriter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class MapperFriends extends Mapper<Text, Text, NullWritable, Text> {
    private String selectedUser;

    @Override
    protected void setup(Context context)throws IOException, InterruptedException {
        super.setup(context);
        selectedUser = context.getConfiguration().get("user");
    }

    @Override
    public void map(Text user1, Text user2, Context context) throws IOException, InterruptedException {
        if(user1.toString().equals(selectedUser)){
            context.write(NullWritable.get(), user2);
        }
        if(user2.toString().equals(selectedUser)){
            context.write(NullWritable.get(), user1);
        }
    }
}
