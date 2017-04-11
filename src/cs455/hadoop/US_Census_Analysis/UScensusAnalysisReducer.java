package cs455.hadoop.US_Census_Analysis;

import com.sun.xml.internal.txw2.TXW;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives ("U.S.", list<Segment> ) pairs.
 * Sums up individual fields according to name of states. Emits <Name of State, List<Answer> pairs.
 */
public class UScensusAnalysisReducer extends Reducer<Text, Iterable<Segment>, Text, Text> {

    HashMap<String, HashMap<String, Integer>> ans;
    ArrayList<String> problems;


    private void initializationOfAnswer(String state){
        //initial the various answer for one state
        HashMap<String, Integer> ansPerState = new HashMap<>();
        ans.put(state, ansPerState);

        //sum for Q1
        ansPerState.put("ownerOccupied", 0);
        ansPerState.put("renterOccupied", 0);


        //sum for Q2
        ansPerState.put("totalMen", 0);
        ansPerState.put("totalWomen", 0);

        ansPerState.put("neverMarriedMen", 0);
        ansPerState.put("marriedMen", 0);
        ansPerState.put("neverMarriedWomen", 0);
        ansPerState.put("marriedWomen", 0);

        //sum for Q3
        ansPerState.put("aged18andBelow18Men", 0);
        ansPerState.put("aged19to29Men", 0);
        ansPerState.put("aged30to39Men", 0);
        ansPerState.put("populationOfHispanicsMen", 0);
        ansPerState.put("totalMen", 0);
        ansPerState.put("aged18andBelow18Women", 0);
        ansPerState.put("aged19to29Women", 0);
        ansPerState.put("aged30to39Women", 0);
        ansPerState.put("populationOfHispanicsWomen", 0);
        ansPerState.put("totalWomen", 0);


        //problems insertion:
       // problems.add("percentage of residnce were rented vs. owned");



        /*
        //sum for Q4
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        */
    }

    protected void reduce(Text key, Iterable<Segment> values, Context context) throws IOException, InterruptedException {

        //define the Hashtable ans
        ans = new HashMap<>();
        problems = new ArrayList<>();

        for (Segment seg : values) {
            String state = seg.getState().toString();
            if (!ans.containsKey(state))
                initializationOfAnswer(state);
            HashMap<String,Integer> ansPerState = ans.get(state);
            //Q1:
            Tenure tenure = seg.getTenure();
            if (tenure!=null){
                ansPerState.put("ownerOccupied", ansPerState.get("ownerOccupied") + tenure.getOwnerOccupied().get());
                ansPerState.put("renterOccupied", ansPerState.get("renterOccupied") + tenure.getRenterOccupied().get());
            }
            //Q2:
            PopulationBySex populationBySex = seg.getPopulationBySex();
            if (populationBySex!=null) {
                ansPerState.put("totalMen", ansPerState.get("totalMen") + populationBySex.getTotalMen().get());
                ansPerState.put("totalWomen", ansPerState.get("totalWomen") + populationBySex.getTotalWomen().get());
            }


            /*
            //Q3:
            GenderByMaritalStatus genderByMaritalStatus = seg.getGenderByMaritalStatus();
            if (genderByMaritalStatus!=null){
                ansPerState.put("neverMarriedMen;" ,ansPerState.get("neverMarriedMen;" ) + genderByMaritalStatus.getNeverMarriedMen().get());
                ansPerState.put("neverMarriedWomen",ansPerState.get("neverMarriedWomen") + genderByMaritalStatus.getNeverMarriedWomen().get());
                ansPerState.put("marriedMen"       ,ansPerState.get("marriedMen"       ) + genderByMaritalStatus.getMarriedMen().get());
                ansPerState.put("marriedWomen"     ,ansPerState.get("marriedWomen"     ) + genderByMaritalStatus.getMarriedWomen().get());
            }
            */
        }



        //print out
        Text spaceValue = new Text("");

        //Q1

        //print problem
        context.write(new Text("Q1: Percentage of residences were rented vs. owned"), spaceValue);


        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer renterOccupied = tableForOneState.get("renterOccupied");
            Integer ownerOccupied = tableForOneState.get("ownerOccupied");

            //statistical arithmetics
            Double rentedVSOwned = renterOccupied*1.0 / ownerOccupied;
            rentedVSOwned = (1-rentedVSOwned)*100;

            //convert to String str
            String str = Double.valueOf(rentedVSOwned)+"%";
            Text result = new Text(str);

            //result print (with format)
            context.write(state, result);
        }

        //Q2

        //print problem
        context.write(new Text("Q2: Percentage of people never married"), spaceValue);

        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer totalMen = tableForOneState.get(  "totalMen");
            Integer totalWomen = tableForOneState.get("totalWomen");
            Integer   neverMarriedMen = tableForOneState.get("neverMarriedMen");  //TODO: delete married men and women
            Integer neverMarriedWomen = tableForOneState.get("neverMarriedWomen");

            //statistical arithmetics
            Double singleMenRate   = neverMarriedMen *1.0 / totalMen;
            Double singleWomenRate = neverMarriedWomen *1.0 / totalWomen;
            singleMenRate = (1-singleMenRate)*100;
            singleWomenRate = (1-singleWomenRate)*100;


            //convert to String str
            String strMen = Double.valueOf(singleMenRate)+"%";
            String strWomen = Double.valueOf(singleWomenRate)+"%";
            Text resultMen = new Text(strMen);
            Text resultWomen = new Text(strWomen);



            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    Men"), resultMen);
            context.write(new Text("    Women"), resultWomen);
        }


    }
}

