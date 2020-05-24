package Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import static Jobs.Step1Grams.getDecade;

public class Step2Arrange {
    public static class MapperClassStep2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("-------- Mapper 2 - Step 2 --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String newValue = split[1];
//            System.out.println("Mapper 2 key ------------"+newKey);
//            System.out.println("Mapper 2 value ------------"+newValue);
            context.write(new Text(newKey), new Text(newValue));
        }
    }
    public static class ReducerClassStep2 extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            //if the key is of type <w1 w2 decade, cw1*w2>, pass it forward
//            System.out.println("------------------------Joined Reducer ------------------");
//            System.out.println("reducer working on this key: " +key);
            String[] keySplit = key.toString().split(" ");
            if(keySplit.length>2){
                for(Text value: values){
                    context.write(key,value);
//                    System.out.println("reducer 2 wrote to context key: " +key+" value: " +value);
                }
            }
            else{ //key is of type <w1 decade>
                String w = keySplit[0];
//                System.out.println("This is w in reducer: " + w);
                ArrayList<String> newKeys = new ArrayList<>();
                String c1 ="";
                ArrayList<String> wordindexes = new ArrayList<>();
                for(Text value:values){
//                    System.out.println("reducer curr value: "+value);
                    String[] valueSplit = value.toString().split(" ");
                    if(valueSplit.length>1){ //value of type w1 w2 decade
//                        System.out.println("Value[0] = " + valueSplit[0]);
//                        System.out.println("Value[1] = "+ valueSplit[1] );

                        if(w.equals(valueSplit[0])){ // checking which word of the current value matches to key (1 or 2)
                            newKeys.add(value.toString());
                            wordindexes.add("1");
                        }
                        else{
                            if(w.equals(valueSplit[1])) {
                                newKeys.add(value.toString());
                                wordindexes.add("2");
                            }
                        }
                    }else{ // value of type c1
                        c1 = value.toString();
                    }
                }
                if(!newKeys.isEmpty() && !c1.isEmpty()) {
                    for(int i=0; i<newKeys.size(); i++){
                        String newKey = newKeys.get(i);
                        String wordIndex = wordindexes.get(i);
                        String newValue = "cw" + wordIndex +" " +c1;
                        context.write(new Text(newKey),new Text(newValue));// write (w1 w2 decade, cw1/cw2)
//                        System.out.println("Reducer 2 wrote to context key: " +newKey+ " value: " +newValue);
                    }
                }

            }
        }
    }

    public static class PartitionerClass2 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


}
