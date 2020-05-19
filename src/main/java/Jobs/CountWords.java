package Jobs;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class CountWords {

    public static class MapperClass extends Mapper<LongWritable, Text, MapWritable, LongWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            MapWritable key = new MapWritable();
            key.put(new Text(splitted[0]),getDecade(splitted[1])); // (<word,Decade>,occurrences>)
                context.write(key, new LongWritable(Integer.parseInt(splitted[2])));
        }
    }
    /*
    Decade 1 = 1990-1999
    Decade 2 = 2000-2009
    Decade 3 = 2011-2019
     */
    public static class ReducerClass extends Reducer<MapWritable,LongWritable,MapWritable,LongWritable> {
        @Override
        public void reduce(MapWritable key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum)); // (<word,Decade>,occurrences>)
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    private static LongWritable getDecade(String year){
        long decade =0;
        String str = year.substring(0,3);
        switch(str)
        {
            case "199":
                decade =1;
                break;
            case "200":
                decade = 2;
                break;
            case "201":
                decade = 3;
        }
        return new LongWritable(decade);
    }
    public static void main(String[] args) throws Exception {
        System.out.println(getDecade("2019").get());
    }
}
