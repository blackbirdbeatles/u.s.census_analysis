package cs455.hadoop.US_Census_Analysis;

import com.sun.xml.internal.txw2.TXW;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class UScensusAnalysisReducer extends Reducer<Text, Segment, Text, Text> {

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
        ansPerState.put("neverMarriedWomen", 0);

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

            GenderByMaritalStatus genderByMaritalStatus = seg.getGenderByMaritalStatus();
            if (genderByMaritalStatus!=null){
                ansPerState.put("neverMarriedMen" ,ansPerState.get("neverMarriedMen" ) + genderByMaritalStatus.getNeverMarriedMen().get());
                ansPerState.put("neverMarriedWomen",ansPerState.get("neverMarriedWomen") + genderByMaritalStatus.getNeverMarriedWomen().get());
            }

        }



        //Finally, print out answers
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
            Integer totalResidence = renterOccupied + ownerOccupied;
            Double rentedRate = (renterOccupied*1.0 / totalResidence )*100;
            Double ownedRate  = (ownerOccupied*1.0  / totalResidence) *100;

            //convert to String str
            String strRented = Double.valueOf(rentedRate)+"%";
            String strOwned  = Double.valueOf(ownedRate) + "%";
            Text resultRented = new Text(strRented);
            Text resultOwned = new Text(strOwned);

            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    Rented"), resultRented);
            context.write(new Text("    Owned"), resultOwned);
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
            Integer   neverMarriedMen = tableForOneState.get("neverMarriedMen");
            Integer neverMarriedWomen = tableForOneState.get("neverMarriedWomen");

            //statistical arithmetics
            Double singleMenRate   = (neverMarriedMen *1.0 / totalMen) * 100;
            Double singleWomenRate = (neverMarriedWomen *1.0 / totalWomen) * 100;


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


        //Q3

        //print problem
        context.write(new Text("Q3: Hispanic age distribution by gender"), spaceValue);

        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer aged18andBelow18Men   = tableForOneState.get("aged18andBelow18Men"  );
            Integer aged19to29Men         = tableForOneState.get("aged19to29Men"        );
            Integer aged30to39Men         = tableForOneState.get("aged30to39Men"        );
            Integer agedAbove40Men        = tableForOneState.get("agedAbove40Men");
            Integer aged18andBelow18Women = tableForOneState.get("aged18andBelow18Women");
            Integer aged19to29Women       = tableForOneState.get("aged19to29Women"      );
            Integer aged30to39Women       = tableForOneState.get("aged30to39Women"      );
            Integer agedAbove40Women      = tableForOneState.get("agedAbove40Women");
           


            //statistical arithmetics
            int totalMen = aged18andBelow18Men + aged19to29Men + aged30to39Men +agedAbove40Men;
            int totalWomen = aged18andBelow18Women + aged19to29Women + aged30to39Women +agedAbove40Women;
            Double a18MenRate   = (aged18andBelow18Men *1.0 / totalMen) * 100;
            Double a19MenRate   = (aged19to29Men *1.0 / totalMen) * 100;
            Double a30MenRate   = (aged30to39Men *1.0 / totalMen) * 100;

            Double a18WomenRate   = (aged18andBelow18Women *1.0 / totalWomen) * 100;
            Double a19WomenRate   = (aged19to29Women *1.0 / totalWomen) * 100;
            Double a30WomenRate   = (aged30to39Women *1.0 / totalWomen) * 100;


            //convert to String str
            String strA18Men = Double.valueOf(a18MenRate)+"%";
            String strA19Men = Double.valueOf(a19MenRate)+"%";
            String strA30Men = Double.valueOf(a30MenRate)+"%";

            String strA18Women = Double.valueOf(a18WomenRate)+"%";
            String strA19Women = Double.valueOf(a19WomenRate)+"%";
            String strA30Women = Double.valueOf(a30WomenRate)+"%";

            Text resultA18Men = new Text(strA18Men);
            Text resultA19Men = new Text(strA19Men);
            Text resultA30Men = new Text(strA30Men);

            Text resultA18Women = new Text(strA18Women);
            Text resultA19Women = new Text(strA19Women);
            Text resultA30Women = new Text(strA30Women);


            //result print (with format)
            context.write(state, spaceValue);

            context.write(new Text("    Aged 0-18  Men"), resultA18Men);
            context.write(new Text("    Aged 19-29 Men"), resultA19Men);
            context.write(new Text("    Aged 30-39 Men"), resultA30Men);


            context.write(new Text("    Aged 0-18  Women"), resultA18Women);
            context.write(new Text("    Aged 19-29 Women"), resultA19Women);
            context.write(new Text("    Aged 30-39 Women"), resultA30Women);
        }






    }
}

