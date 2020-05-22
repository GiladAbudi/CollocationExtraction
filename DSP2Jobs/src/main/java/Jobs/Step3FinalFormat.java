package Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step3FinalFormat {
    public static class MapperStep3 extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text lineId, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("-------- Mapper 3 - Step 3 --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String newValue = split[1];
            System.out.println("Mapper 3 key ------------" + newKey);
            System.out.println("Mapper 3 value ------------" + newValue);
            context.write(new Text(newKey), new Text(newValue));
        }

    }

    public static class ReducerStep3 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cw1 = "";
            String cw2 = "";
            String cw1w2 = "";
            System.out.println("--------------Reducer 3 -----------------");
            System.out.println("reducer 3  current key = " + key);
            for (Text value : values) {
                System.out.println("reducer 3 current value = " + value);
                String[] valueSplit = value.toString().split(" ");
                if (valueSplit.length < 2) {
                    cw1w2 = value.toString();
                } else {
                    if (valueSplit[0].equals("cw1")) {
                        cw1 = valueSplit[1];
                    } else {
                        if (valueSplit[0].equals("cw2")) {
                            cw2 = valueSplit[1];
                        }
                    }
                }
            }
            if(!cw1.isEmpty() && !cw2.isEmpty() && !cw1w2.isEmpty()) {
                String newValue = cw1 + " " + cw2 + " " + cw1w2;
                System.out.println("Reducer 3 writes to context key: " + key + " Value: " + newValue);
                context.write(key, new Text(newValue));
            }

        }

    }

    public static class PartitionerClass3 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
