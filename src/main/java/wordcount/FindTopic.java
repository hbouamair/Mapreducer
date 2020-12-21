package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class FindTopic extends Reducer<Text, IntWritable, Text, IntWritable> {

     List<String> economicwords = new ArrayList<String>();
     List<String> socialwords = new ArrayList<String>();
     List<String> politicwords = new ArrayList<String>();

    private HashMap<String,Integer> counter = new HashMap<String,Integer>();


     @Override
     protected void setup(Context context) throws IOException, InterruptedException {

          parsePolitics("src/main/java/wordcount/politics.txt");
          parseEconomic("src/main/java/wordcount/economic.txt");
          parseSocial("src/main/java/wordcount/social.txt");

          counter.put("economic",0);
          counter.put("social",0);
          counter.put("politic",0);

     }

     @Override
     protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int sum = 0 ;
          for(IntWritable value : values){
               sum += 1  ;
          }

          if (economicwords.contains(key.toString())){
               counter.put("economic",counter.get("economic")+sum);
          }
          if (politicwords.contains(key.toString())){
               counter.put("politic",counter.get("politic")+sum);
          }
          if (socialwords.contains(key.toString())){
               counter.put("social",counter.get("social")+sum);

          }

     }


     @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
           int max = 0 ;
           String topic = "" ;

           for (Map.Entry<String,Integer> entry : counter.entrySet()){
               if (entry.getValue() > max ) {
                   max = entry.getValue();
                   topic = entry.getKey();
               }


               System.out.println(entry.getValue()+entry.getKey());
           }



     }

     private void parsePolitics(String politicwordsFilePath){
          try {
               BufferedReader fis = new BufferedReader(new FileReader(
                       new File(politicwordsFilePath)));
               String word;
               while ((word = fis.readLine()) != null) {
                    politicwords.add(word);
               }
          } catch (IOException ioe) {
               System.err.println("Caught exception parsing cached file '"
                       + politicwordsFilePath + "' : " + StringUtils.stringifyException(ioe));
          }

     }

     private void parseSocial(String socialWordsFilePath) {
          try {
               BufferedReader fis = new BufferedReader(new FileReader(
                       new File(socialWordsFilePath)));
               String Word;
               while ((Word = fis.readLine()) != null) {
                    socialwords.add(Word);
               }
          } catch (IOException ioe) {
               System.err.println("Caught exception parsing cached file '"
                       + socialWordsFilePath + "' : " + StringUtils.stringifyException(ioe));
          }
     }

     private void parseEconomic(String ecoWordsFilePath) {
          try {
               BufferedReader fis = new BufferedReader(new FileReader(
                       new File(ecoWordsFilePath)));
               String Word;
               while ((Word = fis.readLine()) != null) {
                    economicwords.add(Word);
               }
          } catch (IOException ioe) {
               System.err.println("Caught exception parsing cached file '"
                       + ecoWordsFilePath + "' : " + StringUtils.stringifyException(ioe));
          }
     }
}
