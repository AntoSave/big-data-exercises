package it.unisa.hpc.hadoop.homework4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DateIncomeWritable implements Writable{
    private String date;
    private double income;
    
    public DateIncomeWritable() {
        this.date = "";
        this.income = Double.MIN_VALUE;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        date = arg0.readUTF();
        income = arg0.readDouble();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeUTF(date);
        arg0.writeDouble(income);
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    @Override
    public String toString() {
        return "(" + date + " " + Double.toString(income) + ")";
    }
    
}
