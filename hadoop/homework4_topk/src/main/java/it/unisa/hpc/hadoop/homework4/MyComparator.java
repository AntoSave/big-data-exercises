package it.unisa.hpc.hadoop.homework4;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class MyComparator implements Comparator<DateIncomeWritable> {
    @Override
    public int compare(DateIncomeWritable o1, DateIncomeWritable o2) {
        if(o1.getIncome() == o2.getIncome()){ //If incomes are equal we prefer the first date among the two
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate d1 = LocalDate.parse(o1.getDate(), df);
            LocalDate d2 = LocalDate.parse(o2.getDate(), df);
            return -d1.compareTo(d2); //- because the smaller date must be taken
        }
        return o1.getIncome()>o2.getIncome()?1:-1;
    }
}
