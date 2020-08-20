package com.example.batchprocessing;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class BatchProcessingApplication {

	@Autowired
	private JobExplorer jobExplorer;

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job importUserJob;

    @RequestMapping("/job1_JobLauncher")
    String requestJob_JobLauncher() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
        jobLauncher.run(importUserJob, createInitialJobParameterMap());
        return "job1_JobLauncher!";
    }

    @RequestMapping("/job1_ProcessBuilder")
    String requestJob_ProcessBuilder() {
    	// Spring Bootで組み込みWebサーバの自動起動を無効化する方法
    	// https://reasonable-code.com/spring-boot-web-server-disable/
    	ProcessBuilder builder = new ProcessBuilder("C:\\pleiades\\java\\11\\bin\\java", "-Dfile.encoding=UTF-8", "-Dspring.datasource.initialization-mode=never", "-Dspring.main.web-application-type=none", "-Dspring.batch.job.enabled=true", "-Dspring.batch.job.names=importUserJob", "-jar", "target/batch-processing-0.0.1-SNAPSHOT.jar", "opdate=20200817", "run.id=1");

    	// Javaから外部プログラム「7-zip」を呼び出す。
    	// https://qiita.com/nogitsune413/items/48d69054b75ea9afbe5b
    	builder.inheritIO();

    	try {
    		builder.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

        return "job1_ProcessBuilder!";
    }

    @RequestMapping("/jobExplorer")
    String requestJobExplorer() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
        return monitor();
    }

    private JobParameters createInitialJobParameterMap() {
        Map<String, JobParameter> m = new HashMap<>();
        //m.put("opdate", new JobParameter(System.currentTimeMillis()));
        m.put("opdate", new JobParameter("20200817"));
        m.put("run.id", new JobParameter("1"));
        JobParameters p = new JobParameters(m);
        return p;
    }

    private String monitor() {

    	String jobInfomation = "";

    	// (1)
    	JobInstance lastJobInstance = jobExplorer.getLastJobInstance("importUserJob");
    	JobExecution lastJobExecution = jobExplorer.getLastJobExecution(lastJobInstance);
    	JobExecution jobExecution = jobExplorer.getJobExecution(lastJobExecution.getId());

    	// (2)
    	String jobName = jobExecution.getJobInstance().getJobName();
    	Date jobStartTime = jobExecution.getStartTime();
    	Date jobEndTime = jobExecution.getEndTime();
    	BatchStatus jobBatchStatus = jobExecution.getStatus();
    	String jobExitCode = jobExecution.getExitStatus().getExitCode();

    	jobInfomation += "jobName:" +  jobName + "<br>";
    	jobInfomation += "jobStartTime:" +  jobStartTime + "<br>";
    	jobInfomation += "jobEndTime:" +  jobEndTime + "<br>";
    	jobInfomation += "jobBatchStatus:" +  jobBatchStatus + "<br>";
    	jobInfomation += "jobExitCode:" +  jobExitCode + "<br><br>";

    	// omitted.

    	// (3)
    	for(StepExecution s : jobExecution.getStepExecutions()){
    		String stepName = s.getStepName();
    		Date stepStartTime = s.getStartTime();
    		Date stepEndTime = s.getEndTime();
    		BatchStatus stepStatus = s.getStatus();
    		String stepExitCode = s.getExitStatus().getExitCode();
    		int readCount = s.getReadCount();
    		int readSkipCount = s.getReadSkipCount();
    		int processSkipCount = s.getProcessSkipCount();
    		int writeCount = s.getWriteCount();
    		int writeSkipCount = s.getWriteSkipCount();
    		int commitCount = s.getCommitCount();
    		int rollbackCount = s.getRollbackCount();
    		int filterCount = s.getFilterCount();
    		int skipCount = s.getSkipCount();

    		jobInfomation += "\t" + "stepName:" +  stepName + "<br>";
    		jobInfomation += "\t" + "stepStartTime:" +  stepStartTime + "<br>";
    		jobInfomation += "\t" + "stepEndTime:" +  stepEndTime + "<br>";
    		jobInfomation += "\t" + "stepStatus:" +  stepStatus + "<br>";
    		jobInfomation += "\t" + "stepExitCode:" +  stepExitCode + "<br>";
    		jobInfomation += "\t" + "readCount:" +  readCount + "<br>";
    		jobInfomation += "\t" + "readSkipCount:" +  readSkipCount + "<br>";
    		jobInfomation += "\t" + "processSkipCount:" +  processSkipCount + "<br>";
    		jobInfomation += "\t" + "writeCount:" +  writeCount + "<br>";
    		jobInfomation += "\t" + "writeSkipCount:" +  writeSkipCount + "<br>";
    		jobInfomation += "\t" + "commitCount:" +  commitCount + "<br>";
    		jobInfomation += "\t" + "rollbackCount:" +  rollbackCount + "<br>";
    		jobInfomation += "\t" + "filterCount:" +  filterCount + "<br>";
    		jobInfomation += "\t" + "skipCount:" +  skipCount + "<br><br>";
    	}

    	return jobInfomation;
    }

	public static void main(String[] args) throws Exception {
		//System.exit(SpringApplication.exit(SpringApplication.run(BatchProcessingApplication.class, args)));
		SpringApplication.run(BatchProcessingApplication.class, args);
	}
}
