package Jobs;

import Tools.ComparableKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step4SortAndDisplay {
    public static class MapperClassStep4 extends Mapper<LongWritable, Text, ComparableKey, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("-------- Mapper 4 - Step 4 --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String likelihood = split[1];
//            System.out.println("Mapper 4 key ------------" + newKey);
//            System.out.println("Mapper 4 value ------------" + likelihood);
            String[] keySplit = newKey.split(" ");
            String w1 = keySplit[0];
            String w2 = keySplit[1];
            String decade = keySplit[2];
            context.write(new ComparableKey(w1, w2, decade, likelihood), new Text(likelihood));
        }

    }

    public static class ReducerStep3 extends Reducer<ComparableKey, Text, Text, Text> {
        private String writeDecade;
        private int decade_Counter;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            decade_Counter = 0;
            writeDecade = "";
        }
        @Override
        public void reduce(ComparableKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String currDecade = key.getDecade().toString();
            if(currDecade!= null && !currDecade.equals(writeDecade)) {
                writeDecade = currDecade;
                decade_Counter =0;
            }
            if(decade_Counter<100) {
                decade_Counter++;
                System.out.println("Reducer 4 writes on decade " + currDecade + "collocation number "+decade_Counter);
                context.write(new Text(currDecade +" #"+decade_Counter + " " +key.getW1().toString() +" "+ key.getW2().toString()), new Text(key.getLikelihood().toString()));
            }
        }
    }

    public static class PartitionerClass4 extends Partitioner<ComparableKey, Text> {
        @Override
        public int getPartition(ComparableKey key, Text value, int numPartitions) {
            return Integer.parseInt(String.valueOf(key.getDecade().toString().charAt(2))) % numPartitions;
        }
    }

}
