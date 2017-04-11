package cs455.hadoop.US_Census_Analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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

    public Text getState() {
        return state;
    }

    public Tenure getTenure() {
        return tenure;
    }

    public PopulationBySex getPopulationBySex() {
        return populationBySex;
    }

    public GenderByMaritalStatus getGenderByMaritalStatus() {
        return genderByMaritalStatus;
    }

    public AgeDistributionByGender_Hispanic getAgeDistributionByGender_hispanic() {
        return ageDistributionByGender_hispanic;
    }

    public UrbanAndRuralHouseholds getUrbanAndRuralHouseholds() {
        return urbanAndRuralHouseholds;
    }

    public ValueOwnerOccupied getValueOwnerOccupied() {
        return valueOwnerOccupied;
    }

    public ValueOfRental getValueOfRental() {
        return valueOfRental;
    }

    public RoomNumberPerHouse getRoomNumberPerHouse() {
        return roomNumberPerHouse;
    }

    public ElderlyPeople getElderlyPeople() {
        return elderlyPeople;
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
    public Tenure(){
        this.ownerOccupied  = new IntWritable(0);
        this.renterOccupied = new IntWritable(0);
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

    public IntWritable getOwnerOccupied() {
        return ownerOccupied;
    }

    public IntWritable getRenterOccupied() {
        return renterOccupied;
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

    public PopulationBySex(){
        this.totalMen   = new IntWritable(0);
        this.totalWomen = new IntWritable(0);
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

    public IntWritable getTotalMen() {
        return totalMen;
    }

    public IntWritable getTotalWomen() {
        return totalWomen;
    }
}

class GenderByMaritalStatus implements Writable{

    private IntWritable neverMarriedMen;
    private IntWritable neverMarriedWomen;


    public GenderByMaritalStatus(IntWritable neverMarriedMen, IntWritable neverMarriedWomen) {
        neverMarriedMen   = neverMarriedMen;
        neverMarriedWomen = neverMarriedWomen;
    }

    public GenderByMaritalStatus(){
        this.neverMarriedMen   = new IntWritable(0);
        this.neverMarriedWomen = new IntWritable(0);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        neverMarriedMen.readFields(in);
        neverMarriedWomen.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        neverMarriedMen.write(out);
        neverMarriedWomen.write(out);
    }

    public IntWritable getNeverMarriedMen() {
        return neverMarriedMen;
    }

    public IntWritable getNeverMarriedWomen() {
        return neverMarriedWomen;
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
    public AgeDistributionByGender_Hispanic(){
         aged18andBelow18Men       = new IntWritable(0);
         aged19to29Men             = new IntWritable(0);
         aged30to39Men             = new IntWritable(0);
         populationOfHispanicsMen  = new IntWritable(0);
         totalMen                  = new IntWritable(0);
         aged18andBelow18Women     = new IntWritable(0);
         aged19to29Women           = new IntWritable(0);
         aged30to39Women           = new IntWritable(0);
         populationOfHispanicsWomen= new IntWritable(0);
         totalWomen                = new IntWritable(0);
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

    public IntWritable getAged18andBelow18Men() {
        return aged18andBelow18Men;
    }

    public IntWritable getAged19to29Men() {
        return aged19to29Men;
    }

    public IntWritable getAged30to39Men() {
        return aged30to39Men;
    }

    public IntWritable getPopulationOfHispanicsMen() {
        return populationOfHispanicsMen;
    }

    public IntWritable getTotalMen() {
        return totalMen;
    }

    public IntWritable getAged18andBelow18Women() {
        return aged18andBelow18Women;
    }

    public IntWritable getAged19to29Women() {
        return aged19to29Women;
    }

    public IntWritable getAged30to39Women() {
        return aged30to39Women;
    }

    public IntWritable getPopulationOfHispanicsWomen() {
        return populationOfHispanicsWomen;
    }

    public IntWritable getTotalWomen() {
        return totalWomen;
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


    public UrbanAndRuralHouseholds(){

       urbanHouseholds = new IntWritable(0);
       ruralHouseholds = new IntWritable(0);
       totalHouseholds = new IntWritable(0);

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

    public IntWritable getUrbanHouseholds() {
        return urbanHouseholds;
    }

    public IntWritable getRuralHouseholds() {
        return ruralHouseholds;
    }

    public IntWritable getTotalHouseholds() {
        return totalHouseholds;
    }
}

//Q5: median value of houses occupied by owners, 20 ranges of value
class ValueOwnerOccupied implements Writable{

    private ArrayList<IntWritable> value;

    public ValueOwnerOccupied(ArrayList<IntWritable> value) {
        this.value = value;
    }

    public ValueOwnerOccupied(){
        for (int i = 0; i < 20; i++)
            value.add(new IntWritable(0));
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        for (int i = 0; i < 20; i++)
            value.get(i).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        for (int i = 0; i < 20; i++)
            value.get(i).write(out);

    }


    public ArrayList<IntWritable> getValue() {
        return value;
    }
}

//Q6: median pay of rent, 17 ranges of rental
class ValueOfRental implements Writable{


    private ArrayList<IntWritable> value;

    public ValueOfRental(ArrayList<IntWritable> value) {
        this.value = value;
    }

    public ValueOfRental(){
        for (int i = 0; i < 17; i++)
            value.add(new IntWritable(0));
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        for (int i = 0; i < 17; i++)
            value.get(i).readFields(in);


    }

    @Override
    public void write(DataOutput out) throws IOException{
        for (int i = 0; i < 17; i++)
            value.get(i).write(out);

    }

    public ArrayList<IntWritable> getValue() {
        return value;
    }
}

//Q7: 95th percentile of the number of rooms per house, 9 kinds of houses
class RoomNumberPerHouse implements Writable{

    private ArrayList<IntWritable> distriburion;

    public RoomNumberPerHouse(ArrayList<IntWritable> distriburion) {

        this.distriburion = distriburion;

    }

    public RoomNumberPerHouse(){
        for (int i=0; i < 9; i++)
            distriburion.add(new IntWritable(0));
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        for (int i=0; i < 9; i++)
            distriburion.get(i).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        for (int i=0; i < 9; i++)
            distriburion.get(i).write(out);
    }


    public ArrayList<IntWritable> getDistriburion() {
        return distriburion;
    }
}

//Q8: elderly people rate

class StatePopulation implements Writable {

    private IntWritable statePopulation;

    public StatePopulation(IntWritable statePopulation) {
        this.statePopulation = statePopulation;
    }

    public StatePopulation(){
        statePopulation = new IntWritable(0);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        statePopulation.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        statePopulation.write(dataOutput);
    }
}


//elderly: age more than 85 (exclusive)
class ElderlyPeople implements Writable{

    private IntWritable agedOver85population;

    public ElderlyPeople(IntWritable agedOver85population) {
        this.agedOver85population = agedOver85population;
    }
    public ElderlyPeople(){
        agedOver85population = new IntWritable(0);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        agedOver85population.readFields(dataInput);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        agedOver85population.write(dataOutput);
    }

    public IntWritable getAgedOver85population() {
        return agedOver85population;
    }
}




