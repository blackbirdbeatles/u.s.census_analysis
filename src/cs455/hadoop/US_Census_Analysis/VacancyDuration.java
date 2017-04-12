package cs455.hadoop.US_Census_Analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by MyGarden on 4/12/17.
 */
public class VacancyDuration implements Writable {
    private ArrayList<IntWritable> duration;

    public VacancyDuration(ArrayList<IntWritable> duration){
        this.duration = duration;
    }
    public VacancyDuration(){
        duration = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            duration.add(new IntWritable(0));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 3; i++)
            duration.get(i).readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < 3; i++)
            duration.get(i).write(dataOutput);

    }

    public ArrayList<IntWritable> getDuration() {
        return duration;
    }
}
