package com.example.batchprocessing;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;
	// end::setup[]

	// tag::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Person> reader() {
		return new FlatFileItemReaderBuilder<Person>()
			.name("personItemReader")
			.resource(new ClassPathResource("sample-data.csv"))
			.delimited()
			.names(new String[]{"firstName", "lastName"})
			.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
				setTargetType(Person.class);
			}})
			.build();
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

    @Bean
    public MyBatisBatchItemWriter<Person> writer() {
        return new MyBatisBatchItemWriterBuilder<Person>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.batchprocessing.PersonMapper.insertPerson")
                .build();
    }
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(step01_CSV_to_DB())
			.next(flow02_DB_to_DB())
			.next(step03_DB_to_CSV())
			.end()
			.build();
	}

	@Bean
	public Step step01_CSV_to_DB() {
		SynchronizedItemStreamReader<Person> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader());

		return stepBuilderFactory.get("step01_CSV_to_DB")
			.<Person, Person> chunk(2)
			.reader(synchronizedReader)
			.writer(writer())
			.taskExecutor(taskExecutor())
			.build();
	}

	@Bean
	public Flow flow02_DB_to_DB() {
	    return new FlowBuilder<SimpleFlow>("splitFlow02_DB_to_DB")
	            .split(taskExecutor())
	            .add(new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_01").start(step02_DB_to_DB_01()).build(), new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_02").start(step02_DB_to_DB_02()).build())
	            .build();
	}

	@Bean
	public Step step02_DB_to_DB_01() {
		return stepBuilderFactory.get("step02_DB_to_DB_01")
			.<Person, Person> chunk(3)
			.reader(new MyBatisCursorItemReaderBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .queryId("com.example.batchprocessing.PersonMapper.findPersonByName")
	                .parameterValues(new HashMap<String, Object>() {{put("name", "J");}})
	                .build())
			.processor(processor())
			.writer(new MyBatisBatchItemWriterBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .statementId("com.example.batchprocessing.PersonMapper.savePerson")
	                .build())
			.build();
	}

	@Bean
	public Step step02_DB_to_DB_02() {
		return stepBuilderFactory.get("step02_DB_to_DB_02")
			.<Person, Person> chunk(3)
			.reader(new MyBatisCursorItemReaderBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .queryId("com.example.batchprocessing.PersonMapper.findPersonByName")
	                .parameterValues(new HashMap<String, Object>() {{put("name", "Z");}})
	                .build())
			.processor(processor())
			.writer(new MyBatisBatchItemWriterBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .statementId("com.example.batchprocessing.PersonMapper.savePerson")
	                .build())
			.build();
	}

	@Bean
	public Step step03_DB_to_CSV() {
		return stepBuilderFactory.get("step03_DB_to_CSV")
			.<Person, Person> chunk(2)
			.reader(new MyBatisCursorItemReaderBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .queryId("com.example.batchprocessing.PersonMapper.findAllPerson")
	                .build())
			.writer(new FlatFileItemWriterBuilder<Person>()
					.headerCallback(new WriteHeaderFlatFileFooterCallback())
					.lineSeparator("\r\n")
           			.name("itemWriter")
           			.resource(new FileSystemResource("target/test-outputs/output.txt"))
           			//.lineAggregator(new PassThroughLineAggregator<>())
           			.delimited()
    				.delimiter(",")
    				.names(new String[] {"firstName", "lastName"})
           			.build())
			.build();
	}
	// end::jobstep[]

	public static class WriteHeaderFlatFileFooterCallback implements FlatFileHeaderCallback {
	    @Override
	    public void writeHeader(Writer writer) throws IOException {
	        // (2)
	        writer.write("omitted");
	    }
	}


	@Bean
	public TaskExecutor taskExecutor(){
	    return new SimpleAsyncTaskExecutor("spring_batch");
	}

}
