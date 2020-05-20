package Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static Jobs.SingleWordsStep1.getDecade;

public class DoubleWordStep2 {
    public static class MapperClass2Gram extends Mapper<LongWritable, Text, Text, Text> {
        // in - 2-gram. out - (w1 w2 decade,c1c2)
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            System.out.println("--------------Mapper 2.1 - 2Gram----------------");
            System.out.println("Line = " +line);
            String[] splitted = line.toString().split("\t");
            String[] bigram = splitted[0].split(" ");
            Text key = new Text(bigram[0]+" " +bigram[1] +" "+getDecade(splitted[1]));
            System.out.println("key = " + key);
            if(!MainPipeline.stopWords.contains(bigram[0]) && !MainPipeline.stopWords.contains(bigram[1])) {
                context.write(key, new Text(splitted[2]));
            }
        }
    }
    public static class MapperClass1GramOutput extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("-------- Mapper 2.2 - 1Gram results --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String newValue = split[1];
            System.out.println("Mapper 2.2 key ------------"+newKey);
            System.out.println("Mapper 2.2 value ------------"+newValue);
            context.write(new Text(newKey), new Text(newValue));
        }
    }
    public static class CombinerStep2 extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            //if the key is of type <w1 decade, c1> pass it forward - 1-gram mapper output0
            if(key.toString().split(" ").length<2){
                for(Text value: values){
                    context.write(key,value);
                }
            }
            else {
                // if the key is of type <w1 w2 decade,occur(yearly)> - 2-gram mapper output
                // for each pair of words w1 w2, create 3 entries:
                // <w1 decade, w1   w2  decade>
                // <w2 decade, w1   w2  decade>
                // <w1 w2 decade, c1c2>
                int sum = 0;
                for (Text value : values) {
                    sum += Integer.parseInt(value.toString());
                }
                String[] keySplit = key.toString().split(" ");
                String w1 = keySplit[0];
                String w2 = keySplit[1];
                String decade = keySplit[3];
                Text key2 = new Text(w1 + " " + decade);
                Text key3 = new Text(w2 + " " + decade);
                context.write(key, new Text("" + sum)); // (w1 w2 decade, c1c2)
                context.write(key2, key); // (w1 decade, w1 w2 decade)
                context.write(key3, key);// (w2 decade, w1 w2 decade)
            }
        }
    }
    public static class JoinedReducerClass extends Reducer<Text,Text,Text,Text> {
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

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


}
