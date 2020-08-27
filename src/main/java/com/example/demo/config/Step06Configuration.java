package com.example.demo.config;

import org.apache.ibatis.session.SqlSessionFactory;
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
import com.example.demo.item.processor.PersonItemProcessor_ForErrorTest02;
import com.example.demo.listener.CommonItemProcessListener;
import com.example.demo.listener.CommonItemReadListener;
import com.example.demo.listener.CommonItemWriteListener;

@Configuration
@EnableBatchProcessing
public class Step06Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

	@Bean
	public Step step06_DB_to_DB(
			ItemReader<Person> step02ItemReader,
			ItemProcessor<Person, Person> step06ItemProcessor,
			ItemWriter<Person> step02ItemWriter,
			CommonItemReadListener commonItemReadListener,
			CommonItemProcessListener commonItemProcessListener,
			CommonItemWriteListener commonItemWriteListener) {
		return stepBuilderFactory.get("step06_DB_to_DB")
			// チャンクサイズの設定
			.<Person, Person> chunk(4)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(step02ItemReader)

			// データの加工
			// ここにビジネスロジックを記述
			// （サンプルではPerson.firstNameを大文字に変換）
			.processor(step06ItemProcessor)

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(step02ItemWriter)

			.listener(commonItemReadListener)
			.listener(commonItemProcessListener)
			.listener(commonItemWriteListener)

			.build();
	}

	@Bean
	public PersonItemProcessor_ForErrorTest02 step06ItemProcessor() {
		return new PersonItemProcessor_ForErrorTest02();
	}

}
