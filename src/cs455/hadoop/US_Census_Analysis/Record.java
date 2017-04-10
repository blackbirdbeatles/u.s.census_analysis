package cs455.hadoop.US_Census_Analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by MyGarden on 4/9/17.
 */
public class Record implements Writable {

    private Text state;

    private Tenure tenure;
    private PopulationBySex populationBySex;
    private GenderByMaritalStatus genderByMaritalStatus;
    private AgeDistributionByGender_Hispanic ageDistributionByGender_hispanic;
    private UrbanAndRuralHouseholds urbanAndRuralHouseholds;
    private ValueOwnerOccupied valueOwnerOccupied;
    private ValueOfRental valueOfRental ;
    private RoomNumberPerHouse roomNumberPerHouse;
    private ElderlyPeople elderlyPeople;



    public Record(){

    }

    @Override
    public void readFields(DataInput in){

    }

    @Override
    public void write(DataOutput out){

    }


}


//Q1: to find the percentage of residences owned by self or rent
class Tenure implements Writable{

    private IntWritable ownerOccupied;
    private IntWritable renterOccupied;
    private IntWritable sumOfResidences;

    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}


//Q2
class PopulationBySex implements Writable{

    private IntWritable totalMen;
    private IntWritable totalWomen;

    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

class GenderByMaritalStatus implements Writable{

    private IntWritable neverMarriedMen;
    private IntWritable MarriedMen;

    private IntWritable neverMarriedWomen;
    private IntWritable MarriedWomen;



    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}


//Q3:
class AgeDistributionByGender_Hispanic implements Writable{

    //number inclusive
    private IntWritable aged18andBelow18Men;
    private IntWritable aged19to29Men;
    private IntWritable aged30to39Men;
    private IntWritable populationOfHispanicsMen;
    private IntWritable totalMen;

    private IntWritable aged18andBelow18Women;
    private IntWritable aged19to29Women;
    private IntWritable aged30to39Women;
    private IntWritable populationOfHispanicsWomen;
    private IntWritable totalWomen;



    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

//Q4:
class UrbanAndRuralHouseholds implements Writable{

    private IntWritable urbanHouseholds;
    private IntWritable ruralHouseholds;
    private IntWritable totalHouseholds;



    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

//Q5: median value of houses occupied by owners
class ValueOwnerOccupied implements Writable{

    private Text range;
    private IntWritable value;

    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

//Q6: median pay of rent
class ValueOfRental implements Writable{

    private Text range;
    private IntWritable value;

    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

//Q7: 95th percentile of the number of rooms per house
class RoomNumberPerHouse implements Writable{

    private IntWritable numberOfRooms;
    private IntWritable ActualFrequency;

    @Override
    public void readFields(DataInput in) throws IOException{

    }

    @Override
    public void write(DataOutput out) throws IOException{

    }
}

//Q8:
//elderly: age more than 85 (exclusive)
class ElderlyPeople implements Writable{

    private IntWritable population;

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }
}




