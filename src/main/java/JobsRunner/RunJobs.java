package JobsRunner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;
// english 1 gram "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data"
// hebrew 1-gram "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"
public class RunJobs {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://countwords192/count.jar") // This should be a full map reduce application.
                .withMainClass("Jobs.MainPipeline")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data",
                        "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data",
                        "s3://countwords192/output/countOutput1/");

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.2").withEc2KeyName("eilon")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Count words 1-gram")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://countwords192/logs/")
                .withReleaseLabel("emr-5.0.0");


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
