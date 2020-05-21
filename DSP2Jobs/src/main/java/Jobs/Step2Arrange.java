package Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static Jobs.Step1Grams.getDecade;

public class Step2Arrange {
    public static class MapperClassStep2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("-------- Mapper 2 - Step 2 --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String newValue = split[1];
            System.out.println("Mapper 2 key ------------"+newKey);
            System.out.println("Mapper 2 value ------------"+newValue);
            context.write(new Text(newKey), new Text(newValue));
        }
    }
    public static class ReducerClassStep2 extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            //if the key is of type <w1 w2 decade, cw1w2>, pass it forward
            System.out.println("------------------------Joined Reducer ------------------");
            System.out.println("reducer working on this key: " +key);
            String[] keySplit = key.toString().split(" ");
            if(keySplit.length>2){
                for(Text value: values){
                    context.write(key,value);
                }
            }
            else{ //key is of type <w1 decade>
                String w = keySplit[0];
                String newKey = "";
                String cw ="";
                String wordNum="0";
                for(Text value:values){
                    System.out.println("reducer curr value: "+value);
                    String[] valueSplit = value.toString().split(" ");
                    if(valueSplit.length>1){ //value of type w1 w2 decade
                        newKey = value.toString();
                        if(w.equals(valueSplit[0])){
                            wordNum ="1";
                        }
                        else{
                            if(w.equals(valueSplit[1])) {
                                wordNum = "2";
                            }
                            else{
                                continue;
                            }
                        }
                    }else{ // value of type c1
                        cw = "cw" +wordNum+" "+value.toString();
                    }
                    if(!newKey.isEmpty())
                        context.write(new Text(newKey),new Text(cw));// write (w1 w2 decade, cw1/cw2)
                }

            }
        }
    }

    public static class PartitionerClass2 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


}
