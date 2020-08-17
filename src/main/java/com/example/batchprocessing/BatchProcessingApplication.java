package com.example.batchprocessing;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
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
    JobLauncher jobLauncher;

    @Autowired
    Job importUserJob;

    @RequestMapping("/job1")
    String requestJob1() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
        jobLauncher.run(importUserJob, createInitialJobParameterMap());
        return "importUserJob!";
    }

    private JobParameters createInitialJobParameterMap() {
        Map<String, JobParameter> m = new HashMap<>();
        //m.put("opdate", new JobParameter(System.currentTimeMillis()));
        m.put("opdate", new JobParameter("20200817"));
        JobParameters p = new JobParameters(m);
        return p;
    }

	public static void main(String[] args) throws Exception {
		//System.exit(SpringApplication.exit(SpringApplication.run(BatchProcessingApplication.class, args)));
		SpringApplication.run(BatchProcessingApplication.class, args);
	}
}
