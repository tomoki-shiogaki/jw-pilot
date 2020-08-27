package com.example.demo.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.demo.entity.Person;
import com.example.demo.item.processor.PersonItemProcessor;

@Configuration
@EnableBatchProcessing
public class Step02Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

	@Bean
	public Step step02_DB_to_DB(
			ItemReader<Person> step02ItemReader,
			ItemProcessor<Person, Person> step02ItemProcessor,
			ItemWriter<Person> step02ItemWriter) {
		return stepBuilderFactory.get("step02_DB_to_DB")
			// チャンクサイズの設定
			.<Person, Person> chunk(4)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step02ItemReader)

			// データの加工
			// ここにビジネスロジックを記述
			// （サンプルではPerson.firstNameを大文字に変換）
			.processor(step02ItemProcessor)

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(step02ItemWriter)

			.build();
	}

	@Bean
	public MyBatisCursorItemReader<Person> step02ItemReader() {
		return new MyBatisCursorItemReaderBuilder<Person>()
				.sqlSessionFactory(sqlSessionFactory)
				.queryId("com.example.demo.mapper.PersonMapper.findAllPerson")
				.build();
	}

	@Bean
	public PersonItemProcessor step02ItemProcessor() {
		return new PersonItemProcessor();
	}

	@Bean
	public MyBatisBatchItemWriter<Person> step02ItemWriter() {
		return new MyBatisBatchItemWriterBuilder<Person>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.PersonMapper.savePerson")
                .build();
	}

}
