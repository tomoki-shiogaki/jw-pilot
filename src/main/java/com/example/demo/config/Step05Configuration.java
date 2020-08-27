package com.example.demo.config;

import java.util.HashMap;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
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

import com.example.demo.entity.Person;

@Configuration
@EnableBatchProcessing
public class Step05Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

    @Bean
    public Flow flow05_DB_to_DB(
            Step step05_DB_to_DB_01,
            Step step05_DB_to_DB_02) {
        return new FlowBuilder<SimpleFlow>("splitFlow05_DB_to_DB")
                // 非同期用のTaskExecutorを設定
                .split(new SimpleAsyncTaskExecutor("spring_batch"))

                // 並行して実行するステップを登録
                .add(
                        // 名前に"G1"が含まれるレコードを処理対象とするステップ
                        new FlowBuilder<SimpleFlow>("flow05_DB_to_DB_01").start(step05_DB_to_DB_01).build(),

                        // 名前に"G2"が含まれるレコードを処理対象とするステップ
                        new FlowBuilder<SimpleFlow>("flow05_DB_to_DB_02").start(step05_DB_to_DB_02).build()
                    )

                .build();
    }


	@Bean
	public Step step05_DB_to_DB_01(
			ItemReader<Person> step05ItemReader01,
			ItemProcessor<Person, Person> step02ItemProcessor,
			ItemWriter<Person> step02ItemWriter) {
		return stepBuilderFactory.get("step05_DB_to_DB_01")
			// チャンクサイズの設定
			.<Person, Person> chunk(4)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step05ItemReader01)

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
	public Step step05_DB_to_DB_02(
			ItemReader<Person> step05ItemReader02,
			ItemProcessor<Person, Person> step02ItemProcessor,
			ItemWriter<Person> step02ItemWriter) {
		return stepBuilderFactory.get("step05_DB_to_DB_01")
			// チャンクサイズの設定
			.<Person, Person> chunk(4)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step05ItemReader02)

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
	public MyBatisCursorItemReader<Person> step05ItemReader01() {
		return new MyBatisCursorItemReaderBuilder<Person>()
				.sqlSessionFactory(sqlSessionFactory)
				.queryId("com.example.demo.mapper.PersonMapper.findPersonByName")
				.parameterValues(new HashMap<String, Object>() {{put("name", "G1");}})
				.build();
	}

	@Bean
	public MyBatisCursorItemReader<Person> step05ItemReader02() {
		return new MyBatisCursorItemReaderBuilder<Person>()
				.sqlSessionFactory(sqlSessionFactory)
				.queryId("com.example.demo.mapper.PersonMapper.findPersonByName")
				.parameterValues(new HashMap<String, Object>() {{put("name", "G2");}})
				.build();
	}


}
