package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;

public class MainPipeline {
    public static HashSet<String> stopWords = new HashSet<>(Arrays.asList("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone",
            "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an",
            "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at",
            "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind",
            "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call",
            "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do",
            "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty",
            "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen",
            "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
            "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
            "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
            "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last",
            "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine",
            "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never",
            "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of",
            "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves",
            "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed",
            "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so",
            "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten",
            "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore",
            "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through",
            "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
            "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
            "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
            "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your",
            "yours", "yourself", "yourselves"));
    public static void main(String[] args) throws Exception {
        // ------------------------- Step 1 -----------------------
        String path1Gram = args[1];
        String path2Gram=args[2];
        String outputPath = args[3];
        String output1 = outputPath +"Step1Output"+ LocalDateTime.now()+" /";
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1,"Count");
        MultipleInputs.addInputPath(job, new Path(path1Gram), TextInputFormat.class,
                Step1Grams.MapperClass1Gram.class);
        MultipleInputs.addInputPath(job, new Path(path2Gram), TextInputFormat.class,
                Step1Grams.MapperClass2Gram.class);
        job.setJarByClass(Step1Grams.class);
        job.setPartitionerClass(Step1Grams.PartitionerClass.class);
        job.setReducerClass(Step1Grams.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output1));
        job.setOutputFormatClass(TextOutputFormat.class);
        if(job.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
        }
        else{
            System.out.println("Step 1 failed ");
        }
//        // ------------------------- Step 2 -----------------------
//        String output2 = outputPath+"Step2Output"+LocalDateTime.now()+" /";
//        Configuration conf2 = new Configuration();
//        job = Job.getInstance(conf2,"Arrange step1 output");
//        job.setJarByClass(Step2Arrange.class);
//        job.setMapperClass(Step2Arrange.MapperClassStep2.class);
//        job.setPartitionerClass(Step2Arrange.PartitionerClass2.class);
//        job.setReducerClass(Step2Arrange.ReducerClassStep2.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job,new Path(output1));
//        FileOutputFormat.setOutputPath(job, new Path(output2));
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        if(job.waitForCompletion(true)) {
//            System.out.println("Step 2 finished");
//        }
//        else{
//            System.out.println("Step 2 failed ");
//        }

    }

}
