package com.example.batchprocessing.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.example.batchprocessing.dto.Person;

@Configuration
@EnableBatchProcessing
public class Step03Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

	/**
	 * DBからCSVにエクスポートするためのサンプルコード（DB ⇒ CSV）
	 *
	 * 　　<参考サイト>
	 *
	 * 　　Spring Batch - リファレンスドキュメント > ItemReader および ItemWriter > FlatFileItemWriter
	 * 　　https://spring.pleiades.io/spring-batch/docs/current/reference/html/readersAndWriters.html#flatFileItemWriter
	 *
	 *
	 * @return
	 */
	@Bean
	public Step step03_DB_to_CSV(
			ItemReader<Person> step03ItemReader,
			ItemWriter<Person> step03ItemWriter) {
		return stepBuilderFactory.get("step03_DB_to_CSV")
			// チャンクサイズの設定
			.<Person, Person> chunk(10)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step03ItemReader)

			// データの加工（あれば）
			//.processor(processor())

			// データの出力（DTO ⇒ CSV）
			// DTO「Person」をCSVに書き込む
			.writer(step03ItemWriter)

			.build();
	}

	@Bean
	public ItemReader<Person> step03ItemReader(){
	    return new MyBatisCursorItemReaderBuilder<Person>()
	    		.sqlSessionFactory(sqlSessionFactory)
	    		.queryId("com.example.batchprocessing.mapper.PersonMapper.findAllPerson")
	    		.build();
	}

	@Bean
	public ItemWriter<Person> step03ItemWriter(){
	    return new FlatFileItemWriterBuilder<Person>()
       			.name("itemWriter")
       			.resource(new FileSystemResource("target/test-outputs/output.txt"))
       			//.headerCallback(new WriteHeaderFlatFileFooterCallback())
       			.lineSeparator("\r\n")
       			//.lineAggregator(new PassThroughLineAggregator<>())
       			.delimited()
				.delimiter(",")
				.names(new String[] {"firstName", "lastName"})
       			.build();
	}

}
