import org.jets3t.service.security.AWSCredentials;

public class RunJobs {
    AWSCredentials credentials = new PropertiesCredentials(...);
    AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
            .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
            .withMainClass("some.pack.MainClass")
            .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");

    StepConfig stepConfig = new StepConfig()
            .withName("stepname")
            .withHadoopJarStep(hadoopJarStep)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
            .withInstanceCount(2)
            .withMasterInstanceType(InstanceType.M1Small.toString())
            .withSlaveInstanceType(InstanceType.M1Small.toString())
            .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
            .withKeepJobFlowAliveWhenNoSteps(false)
            .withPlacement(new PlacementType("us-east-1a"));

    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
            .withName("jobname")
            .withInstances(instances)
            .withSteps(stepConfig)
            .withLogUri("s3n://yourbucket/logs/");

    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
    String jobFlowId = runJobFlowResult.getJobFlowId();
System.out.println("Ran job flow with id: " + jobFlowId);
}
