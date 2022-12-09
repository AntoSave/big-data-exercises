package it.unisa.hpc.hadoop.homework4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;

public class PairWritable<T extends Writable, S extends Writable> implements Writable {  
    T a;
    S b;
    public static Class aClass, bClass;

    T createContents(Constructor<T> constructor) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return constructor.newInstance();
    }

    public PairWritable() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException{
        a = (T) aClass.newInstance();
        b = (S) bClass.newInstance();
    }

    public PairWritable(T a, S b){
        this.a = a;
        this.b = b;
    }

    public T getFirst(){
        return a;
    }

    public S getSecond(){
        return b;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        a.write(out);
        b.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        a.readFields(in);
        b.readFields(in);
    }

    @Override
    public String toString(){
        return "(" + a.toString() + " " + b.toString() + ")";
    }
    
}
