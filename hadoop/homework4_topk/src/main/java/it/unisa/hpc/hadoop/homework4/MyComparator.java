package it.unisa.hpc.hadoop.homework4;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class MyComparator implements Comparator<PairWritable<Text, DoubleWritable>> {
    @Override
    public int compare(PairWritable<Text, DoubleWritable> o1, PairWritable<Text, DoubleWritable> o2) {
        int result = o1.getSecond().compareTo(o1.getSecond());
        if(result == 0){ //If incomes are equal we prefer the first date among the two
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate d1 = LocalDate.parse(o1.getFirst().toString(), df);
            LocalDate d2 = LocalDate.parse(o1.getFirst().toString(), df);
            return -d1.compareTo(d2); //- because the smaller date must be taken
        }
        return result;
    }
}
