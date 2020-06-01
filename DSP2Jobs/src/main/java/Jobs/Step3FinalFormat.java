package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step3FinalFormat {
    public static class MapperStep3 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("-------- Mapper 3 - Step 3 --------");
            String[] split = value.toString().split("\t");
            String newKey = split[0];
            String newValue = split[1];
//            System.out.println("Mapper 3 key ------------" + newKey);
//            System.out.println("Mapper 3 value ------------" + newValue);
            context.write(new Text(newKey), new Text(newValue));
        }

    }

    public static class ReducerStep3 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration c = context.getConfiguration();
            String data = c.get("COUNTER_N1");
            String[] counterDecade = data.split("\n");
            for (String decade_counter : counterDecade) {
                String[] dec_val = decade_counter.split(" ");// [decade,N]
                context.getCounter("COUNTER_N1", dec_val[0]).increment(Long.parseLong(dec_val[1]));
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cw1 = "";
            String cw2 = "";
            String cw1w2 = "";
//            System.out.println("--------------Reducer 3 -----------------");
//            System.out.println("reducer 3  current key = " + key);
            for (Text value : values) {
//                System.out.println("reducer 3 current value = " + value);
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
            if (cw1.isEmpty()) {
                cw1 = "1";
            }
            if (cw2.isEmpty()) {
                cw2 = "1";
            }
            if (cw1w2.isEmpty()) {
                cw1w2 = "1";
            }
            String[] keySplit = key.toString().split(" ");
            String decade = keySplit[2];
            long N = context.getCounter("COUNTER_N1",decade).getValue();
//            System.out.println("Current N for " +decade+" is : " +N);
            String finalResult = Step3FinalFormat.logLikelihood(Double.parseDouble(cw1), Double.parseDouble(cw2), Double.parseDouble(cw1w2),(double) N);
//            System.out.println("Reducer 3 writes to context key: " + key + " Value: " + finalResult);
            if (!finalResult.equals("NaN")) {
                context.write(key, new Text(finalResult));
            }
        }

    }

    public static class PartitionerClass3 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    private static String logLikelihood(double c1, double c2, double c12, double N) {
        double p = c2 / N;
        double p1 = c12 / c1;
        double p2 = (c2 - c12) / (N - c1);
        double result = logL(c12, c1, p) + logL(c2 - c12, N - c1, p) - logL(c12, c1, p1) - logL(c2 - c12, N - c1, p2);
        return String.valueOf(result*-2) ;
    }

    private static double logL(double k, double n, double x) {
        return Math.log(Math.pow(x, k) * Math.pow(1 - x, n - k));
    }
}
