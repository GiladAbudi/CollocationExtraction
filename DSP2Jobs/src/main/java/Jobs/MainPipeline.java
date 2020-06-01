package Jobs;

import Tools.ComparableKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
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

    public final static  HashSet<String> hebrewStopWords = new HashSet<>(Arrays.asList("של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה"
            , "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
            "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
            "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
            "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"","״","׳",
            "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע",
            "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי",
            "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
            "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין",
            "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"));

    public static void main(String[] args) throws Exception {
        // ------------------------- Step 1 -----------------------
        String path1Gram = args[1];
        String path2Gram = args[2];
        String outputPath = args[3];
        String language =args[4];
        String time = LocalDateTime.now().toString().replace(':','-');
        String output1 = outputPath + "Step1Output"+time+"/";
        Configuration conf1 = new Configuration();
        conf1.set("language",language);
        conf1.set("mapreduce.map.java.opts","-Xmx512m");
        conf1.set("mapreduce.reduce.java.opts","-Xmx1536m");
        conf1.set("mapreduce.map.memory.mb","768");
        conf1.set("mapreduce.reduce.memory.mb","2048");
        conf1.set("yarn.app.mapreduce.am.resource.mb","2048");

        conf1.set("yarn.scheduler.minimum-allocation-mb","256");
        conf1.set("yarn.scheduler.maximum-allocation-mb","12288");
        conf1.set("yarn.nodemanager.resource.memory-mb","12288");
        conf1.set("mapreduce.reduce.shuffle.memory.limit.percent","0.5");

        Job job1 = Job.getInstance(conf1,"Count");
        MultipleInputs.addInputPath(job1, new Path(path1Gram), SequenceFileInputFormat.class,
                Step1Grams.MapperClass1Gram.class);
        MultipleInputs.addInputPath(job1, new Path(path2Gram), SequenceFileInputFormat.class,
                Step1Grams.MapperClass2Gram.class);
        job1.setJarByClass(Step1Grams.class);
        job1.setPartitionerClass(Step1Grams.PartitionerClass.class);
        job1.setCombinerClass(Step1Grams.CombinerClass.class);
        job1.setReducerClass(Step1Grams.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        job1.setOutputFormatClass(TextOutputFormat.class);
        if(job1.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
        }
        else{
            System.out.println("Step 1 failed ");
        }
        String data=getData(job1);
        // ------------------------- Step 2 -----------------------
        String output2 = outputPath+"Step2Output" + time+ "/";
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Step 2 - arrange keys");
        job2.setJarByClass(Step2Arrange.class);
        job2.setMapperClass(Step2Arrange.MapperClassStep2.class);
        job2.setPartitionerClass(Step2Arrange.PartitionerClass2.class);
        job2.setReducerClass(Step2Arrange.ReducerClassStep2.class);
        setJob(job2,output1, output2);
        if(job2.waitForCompletion(true)) {
            System.out.println("Step 2 finished");
        }
        else{
            System.out.println("Step 2 failed ");
        }

        String output3 = outputPath+"Step3Output" + time+ "/";
        Configuration conf3 = new Configuration();
        conf3.set("COUNTER_N1",data);// for gram-1
        Job job3 = Job.getInstance(conf3,"Step 3 - compute log");
        job3.setJarByClass(Step3FinalFormat.class);
        job3.setMapperClass(Step3FinalFormat.MapperStep3.class);
        job3.setPartitionerClass(Step3FinalFormat.PartitionerClass3.class);
        job3.setReducerClass(Step3FinalFormat.ReducerStep3.class);
        setJob(job3,output2, output3);
        if(job3.waitForCompletion(true)) {
            System.out.println("Step 3 finished");
        }
        else{
            System.out.println("Step 3 failed ");
        }
        String output4 = outputPath+"Step4Output" + time+ "/";
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4,"Step 4 - display results");
        job4.setJarByClass(Step4SortAndDisplay.class);
        job4.setMapperClass(Step4SortAndDisplay.MapperClassStep4.class);
        job4.setPartitionerClass(Step4SortAndDisplay.PartitionerClass4.class);
        job4.setReducerClass(Step4SortAndDisplay.ReducerStep3.class);
        setJob(job4,output3, output4);
        job4.setMapOutputKeyClass(ComparableKey.class);
        job4.setMapOutputValueClass(Text.class);
        if(job4.waitForCompletion(true)) {
            System.out.println("Work Complete!");
        }
        else{
            System.out.println("Step 4 failed");
        }

    }

    private static void setJob(Job job,String inputPath, String outputPath) throws IOException {
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }
    private static String getData(Job job) throws IOException {
        String data="";
        Counters counters = job.getCounters();
        CounterGroup countersN= counters.getGroup("COUNTER_N1");
        for (Counter counter : countersN) {
            data+=counter.getName()+" " +counter.getValue()+'\n';
        }
        return data;
    }



}
