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
public class Segment implements Writable {

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


    public Segment(Text state, Tenure tenure, PopulationBySex populationBySex, GenderByMaritalStatus genderByMaritalStatus, AgeDistributionByGender_Hispanic ageDistributionByGender_hispanic, UrbanAndRuralHouseholds urbanAndRuralHouseholds, ValueOwnerOccupied valueOwnerOccupied, ValueOfRental valueOfRental, RoomNumberPerHouse roomNumberPerHouse, ElderlyPeople elderlyPeople) {
        this.state = state;
        this.tenure = tenure;
        this.populationBySex = populationBySex;
        this.genderByMaritalStatus = genderByMaritalStatus;
        this.ageDistributionByGender_hispanic = ageDistributionByGender_hispanic;
        this.urbanAndRuralHouseholds = urbanAndRuralHouseholds;
        this.valueOwnerOccupied = valueOwnerOccupied;
        this.valueOfRental = valueOfRental;
        this.roomNumberPerHouse = roomNumberPerHouse;
        this.elderlyPeople = elderlyPeople;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        state.readFields(dataInput);
        tenure.readFields(dataInput);
        populationBySex.readFields(dataInput);
        genderByMaritalStatus.readFields(dataInput);
        ageDistributionByGender_hispanic.readFields(dataInput);
        this.urbanAndRuralHouseholds.readFields(dataInput);
        this.valueOwnerOccupied.readFields(dataInput);
        this.valueOfRental.readFields(dataInput);
        this.roomNumberPerHouse.readFields(dataInput);
        this.elderlyPeople.readFields(dataInput);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        state.write(dataOutput);
        tenure.write(dataOutput);
        populationBySex.write(dataOutput);
        genderByMaritalStatus.write(dataOutput);
        ageDistributionByGender_hispanic.write(dataOutput);
        this.urbanAndRuralHouseholds.write(dataOutput);
        this.valueOwnerOccupied.write(dataOutput);
        this.valueOfRental.write(dataOutput);
        this.roomNumberPerHouse.write(dataOutput);
        this.elderlyPeople.write(dataOutput);
    }
}


//Q1: to find the percentage of residences owned by self or rent
class Tenure implements Writable{

    private IntWritable ownerOccupied;
    private IntWritable renterOccupied;

    public Tenure(IntWritable ownerOccupied, IntWritable renterOccupied) {
        this.ownerOccupied = ownerOccupied;
        this.renterOccupied = renterOccupied;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ownerOccupied.readFields(dataInput);
        renterOccupied.readFields(dataInput);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException{

        ownerOccupied.write(dataOutput);
        renterOccupied.write(dataOutput);

    }
}


//Q2
class PopulationBySex implements Writable{

    private IntWritable totalMen;
    private IntWritable totalWomen;

    public PopulationBySex(IntWritable totalMen, IntWritable totalWomen) {
        this.totalMen = totalMen;
        this.totalWomen = totalWomen;
    }

    @Override
    public void readFields(DataInput in) throws IOException{

        totalMen.readFields(in);
        totalWomen.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        totalMen.write(out);
        totalWomen.write(out);
    }
}

class GenderByMaritalStatus implements Writable{

    private IntWritable neverMarriedMen;
    private IntWritable marriedMen;

    private IntWritable neverMarriedWomen;
    private IntWritable marriedWomen;


    public GenderByMaritalStatus(IntWritable neverMarriedMen, IntWritable marriedMen, IntWritable neverMarriedWomen, IntWritable marriedWomen) {
        neverMarriedMen   = neverMarriedMen;
        marriedMen        = marriedMen;
        neverMarriedWomen = neverMarriedWomen;
        marriedWomen      = marriedWomen;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        neverMarriedMen.readFields(in);
        marriedMen.readFields(in);
        neverMarriedWomen.readFields(in);
        marriedWomen.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException{
        neverMarriedMen.write(out);
        marriedMen.write(out);
        neverMarriedWomen.write(out);
        marriedWomen.write(out);
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

    public AgeDistributionByGender_Hispanic(IntWritable aged18andBelow18Men, IntWritable aged19to29Men, IntWritable aged30to39Men, IntWritable populationOfHispanicsMen, IntWritable totalMen, IntWritable aged18andBelow18Women, IntWritable aged19to29Women, IntWritable aged30to39Women, IntWritable populationOfHispanicsWomen, IntWritable totalWomen) {
        this.aged18andBelow18Men = aged18andBelow18Men;
        this.aged19to29Men = aged19to29Men;
        this.aged30to39Men = aged30to39Men;
        this.populationOfHispanicsMen = populationOfHispanicsMen;
        this.totalMen = totalMen;
        this.aged18andBelow18Women = aged18andBelow18Women;
        this.aged19to29Women = aged19to29Women;
        this.aged30to39Women = aged30to39Women;
        this.populationOfHispanicsWomen = populationOfHispanicsWomen;
        this.totalWomen = totalWomen;
    }

    @Override
    public void readFields(DataInput in) throws IOException{

       aged18andBelow18Men.readFields(in);
       aged19to29Men.readFields(in);
       aged30to39Men.readFields(in);
       populationOfHispanicsMen.readFields(in);
       totalMen.readFields(in);

       aged18andBelow18Women.readFields(in);
       aged19to29Women.readFields(in);
       aged30to39Women.readFields(in);
       populationOfHispanicsWomen.readFields(in);
       totalWomen.readFields(in);



    }

    @Override
    public void write(DataOutput out) throws IOException{
       aged18andBelow18Men.write(out);
       aged19to29Men.write(out);
       aged30to39Men.write(out);
       populationOfHispanicsMen.write(out);
       totalMen.write(out);

       aged18andBelow18Women.write(out);
       aged19to29Women.write(out);
       aged30to39Women.write(out);
       populationOfHispanicsWomen.write(out);
       totalWomen.write(out);

    }
}

//Q4:
class UrbanAndRuralHouseholds implements Writable{

    private IntWritable urbanHouseholds;
    private IntWritable ruralHouseholds;
    private IntWritable totalHouseholds;

    public UrbanAndRuralHouseholds(IntWritable urbanHouseholds, IntWritable ruralHouseholds, IntWritable totalHouseholds) {
        this.urbanHouseholds = urbanHouseholds;
        this.ruralHouseholds = ruralHouseholds;
        this.totalHouseholds = totalHouseholds;
    }

    @Override
    public void readFields(DataInput in) throws IOException{

        urbanHouseholds.readFields(in);
        ruralHouseholds.readFields(in);
        totalHouseholds.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException{
        
        urbanHouseholds.write(out);
        ruralHouseholds.write(out);
        totalHouseholds.write(out);


    }
}

//Q5: median value of houses occupied by owners
class ValueOwnerOccupied implements Writable{

    private Text range;
    private IntWritable value;

    public ValueOwnerOccupied(Text range, IntWritable value) {
        this.range = range;
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException{

        range.readFields(in);
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
         range.write(out);
         value.write(out);

    }
}

//Q6: median pay of rent
class ValueOfRental implements Writable{

    private Text range;
    private IntWritable value;

    public ValueOfRental(Text range, IntWritable value) {
        this.range = range;
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
         range.readFields(in);
         value.readFields(in);


    }

    @Override
    public void write(DataOutput out) throws IOException{
        range.write(out);
        value.write(out);

    }
}

//Q7: 95th percentile of the number of rooms per house
class RoomNumberPerHouse implements Writable{

    private IntWritable numberOfRooms;
    private IntWritable ActualFrequency;

    public RoomNumberPerHouse(IntWritable numberOfRooms, IntWritable actualFrequency) {
        numberOfRooms   = numberOfRooms;
        ActualFrequency = actualFrequency;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
          numberOfRooms.readFields(in);
        ActualFrequency.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
          numberOfRooms.write(out);
        ActualFrequency.write(out);
    }
}

//Q8:
//elderly: age more than 85 (exclusive)
class ElderlyPeople implements Writable{

    private IntWritable population;

    public ElderlyPeople(IntWritable population) {
        population = population;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        population.readFields(dataInput);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        population.write(dataOutput);
    }
}




