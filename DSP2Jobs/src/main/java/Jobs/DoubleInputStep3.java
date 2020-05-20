//package Jobs;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
//public class DoubleInputStep3 {
//    public static class MapperInput1 extends Mapper<Text, Text, Text, Text> {
//
//        @Override
//        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
//            System.out.println("---------------Mapper 1------------------");
//            System.out.println("Line = " +line);
//            String[] splitted = line.toString().split("");
//    }
//
//        public static class MapperInput2 extends Mapper<Text, Text, Text, Text> {
//
//            @Override
//            public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
//                System.out.println("---------------Mapper 1------------------");
//                System.out.println("Line = " +line);
//                String[] splitted = line.toString().split("\t");
//            }
//    public static class ReducerClass extends Reducer<Text,LongWritable,Text,Text> {
//        @Override
//        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
//            System.out.println("CHECK REDUCER");
//            int sum = 0;
//            for (LongWritable value : values) {
//                sum += value.get();
//            }
//            context.write(key,new Text(" "+sum)); // <word decade,occurrences>
//        }
//    }
//
//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//        @Override
//        public int getPartition(Text key, IntWritable value, int numPartitions) {
//            return key.hashCode() % numPartitions;
//        }
//    }
//}
