package com.example.batchprocessing.config;

import java.util.HashMap;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.example.batchprocessing.dto.Person;
import com.example.batchprocessing.item.processor.PersonItemProcessor;
import com.example.batchprocessing.listener.CommonItemProcessListener;
import com.example.batchprocessing.listener.CommonItemReadListener;
import com.example.batchprocessing.listener.CommonItemWriteListener;

@Configuration
@EnableBatchProcessing
public class Step02Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

	/**
	 * DBのレコードを加工するためのサンプルコード（DB ⇒ (加工) ⇒ DB）
	 *
	 * 「並行ステップ」を利用して加工処理を並列化。
	 *
	 * 　　<参考サイト>
	 *
	 * 　　SpringBatchとMyBatisでDBを定期更新する
	 * 　　https://qiita.com/hysdsk/items/80caee7046308774401d
	 *
	 * 　　Spring Batch - リファレンスドキュメント > スケーリングと並列処理 > 並行ステップ
	 * 　　https://spring.pleiades.io/spring-batch/docs/current/reference/html/scalability.html#scalabilityParallelSteps
	 *
	 *
	 * @return
	 */
	@Bean
	public Flow flow02_DB_to_DB(
			Step step02_DB_to_DB_01,
			Step step02_DB_to_DB_02) {
	    return new FlowBuilder<SimpleFlow>("splitFlow02_DB_to_DB")
	    		// 非同期用のTaskExecutorを設定
	            .split(asyncTaskExecutor())

	            // 並行して実行するステップを登録
	            .add(
	            		// 名前に"J"が含まれるレコードを処理対象とするステップ
	            		new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_01").start(step02_DB_to_DB_01).build()//,

	            		// 名前に"Z"が含まれるレコードを処理対象とするステップ
	            		//new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_02").start(step02_DB_to_DB_02).build()
	            	)

	            .build();
	}

	@Bean
	public Step step02_DB_to_DB_01(
			ItemReader<Person> step02ItemReader01,
			ItemProcessor<Person, Person> step02ItemProcessor,
			ItemWriter<Person> step02ItemWriter,
			CommonItemReadListener commonItemReadListener,
			CommonItemProcessListener commonItemProcessListener,
			CommonItemWriteListener commonItemWriteListener) {
		return stepBuilderFactory.get("step02_DB_to_DB_01")
			// チャンクサイズの設定
			.<Person, Person> chunk(6)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step02ItemReader01)

			// データの加工
			// ここにビジネスロジックを記述
			// （サンプルではPerson.firstNameを大文字に変換）
			.processor(step02ItemProcessor)

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(step02ItemWriter)

			.listener(commonItemReadListener)
			.listener(commonItemProcessListener)
			.listener(commonItemWriteListener)

			.build();
	}

	@Bean
	public Step step02_DB_to_DB_02(
			ItemReader<Person> step02ItemReader02,
			ItemProcessor<Person, Person> step02ItemProcessor,
			ItemWriter<Person> step02ItemWriter,
			CommonItemReadListener commonItemReadListener,
			CommonItemProcessListener commonItemProcessListener,
			CommonItemWriteListener commonItemWriteListener) {
		return stepBuilderFactory.get("step02_DB_to_DB_02")
			// チャンクサイズの設定
			.<Person, Person> chunk(6)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step02ItemReader02)

			// データの加工
			// ここにビジネスロジックを記述
			// （サンプルではPerson.firstNameを大文字に変換）
			.processor(step02ItemProcessor)

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(step02ItemWriter)

			// リスナーを登録
			// （サンプルではエラー箇所を特定できるようにエラー発生時のItem（レコード）をログに出力）
			.listener(commonItemReadListener)
			.listener(commonItemProcessListener)
			.listener(commonItemWriteListener)

			.build();
	}

	@Bean
	public MyBatisCursorItemReader<Person> step02ItemReader01() {
		return new MyBatisCursorItemReaderBuilder<Person>()
				.sqlSessionFactory(sqlSessionFactory)
				.queryId("com.example.batchprocessing.mapper.PersonMapper.findAllPerson")
				//.queryId("com.example.batchprocessing.mapper.PersonMapper.findPersonByName")
				//.parameterValues(new HashMap<String, Object>() {{put("name", "J");}})
				.build();
	}

	@Bean
	public MyBatisCursorItemReader<Person> step02ItemReader02() {
		return new MyBatisCursorItemReaderBuilder<Person>()
				.sqlSessionFactory(sqlSessionFactory)
				.queryId("com.example.batchprocessing.mapper.PersonMapper.findPersonByName")
				.parameterValues(new HashMap<String, Object>() {{put("name", "Z");}})
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
                .statementId("com.example.batchprocessing.mapper.PersonMapper.savePerson")
                .build();
	}

	@Bean
	public TaskExecutor asyncTaskExecutor(){
	    return new SimpleAsyncTaskExecutor("spring_batch");
		//return new SyncTaskExecutor();
	}

}
