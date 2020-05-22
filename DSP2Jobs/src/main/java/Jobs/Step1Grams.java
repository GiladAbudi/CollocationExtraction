package Jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import sun.tools.jar.Main;

public class Step1Grams {

    public static class MapperClass1Gram extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            System.out.println("---------------Mapper 1.1 - 1Gram------------------");
            System.out.println("Line = " +line);
            String[] splitted = line.toString().split("\t");
            Text key = new Text(splitted[0]+" "+getDecade(splitted[1]));
            System.out.println("key = " + key);
            if(!MainPipeline.stopWords.contains(splitted[0])) {
                context.write(key, new Text(splitted[2])); // <word decade, occur>
            }
        }
    }
    public static class MapperClass2Gram extends Mapper<LongWritable, Text, Text, Text> {
        // in - 2-gram. out - (w1 w2 decade,c1c2)
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            System.out.println("--------------Mapper 1.2 - 2Gram----------------");
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
    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keySplit = key.toString().split(" ");
            if(keySplit.length<3){
                System.out.println("-------Reducer 1 -------------");
                System.out.println("key = " + key.toString());
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                context.getCounter("COUNTER_N1",keySplit[1]).increment(sum);
                context.write(key,new Text(""+sum)); // <word decade,occurrences>
            }
            else{
                System.out.println("key = " + key.toString());
                String w1 = keySplit[0];
                String w2 = keySplit[1];
                String decade = keySplit[2];
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                context.write(new Text(w1+" " +decade),key);
                context.write(new Text(w2+" " +decade),key);
                context.write(key,new Text(""+sum));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    public static String getDecade(String year){
        return year.substring(0,3) +"0";
    }
    public static void main(String[] args) throws Exception {
    }
}
