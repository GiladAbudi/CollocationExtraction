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

public class SingleWordsStep1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            System.out.println("---------------Mapper 1------------------");
            System.out.println("Line = " +line);
            String[] splitted = line.toString().split("\t");
            Text key = new Text(splitted[0]+" "+getDecade(splitted[1]));
            System.out.println("key = " + key);
            if(!MainPipeline.stopWords.contains(splitted[0])) {
                context.write(key, new LongWritable(Integer.parseInt(splitted[2]))); // <word decade, occur>
            }
        }
    }
    public static class ReducerClass extends Reducer<Text,LongWritable,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            System.out.println("CHECK REDUCER");
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new Text(" "+sum)); // <word decade,occurrences>
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
