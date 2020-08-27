package com.example.demo.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.example.demo.entity.Person;

@Configuration
@EnableBatchProcessing
public class Step01Configuration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;

	/**
	 * CSVをDBにインポートするためのサンプルコード（CSV ⇒ DB）
	 *
	 * 　　<参考サイト>
	 *
	 * 　　Spring Batchのアーキテクチャ > Spring Batchの基本構造
	 * 　　https://terasoluna-batch.github.io/guideline/5.0.0.RELEASE/ja/Ch02_SpringBatchArchitecture.html#Ch02_SpringBatchArch_Overview_BasicStructure
	 * 　　
	 * 　　Spring Batch - リファレンスドキュメント > ItemReader および ItemWriter > FlatFileItemReader
	 * 　　https://spring.pleiades.io/spring-batch/docs/current/reference/html/readersAndWriters.html#flatFileItemReader
	 *
	 * @return
	 */
	@Bean
	public Step step01_CSV_to_DB(
			ItemReader<Person> step01ItemReader,
			ItemWriter<Person> step01ItemWriter) {

		return stepBuilderFactory
			// ステップ名？(ログ出力や実行情報などを識別するために使用される？)
			.get("step01_CSV_to_DB")

			// チャンクサイズの設定
			// この単位でDBにコミットされる
			// （チャンクサイズ4、データ総件数10の場合、コミット回数は3回）
			.<Person, Person> chunk(4)

			// データの入力（CSV ⇒ DTO）
			// CSVファイルの読み込みを行うクラス「FlatFileItemReader」を使用してCSVファイルの各レコードをDTO「Person」に変換
			// 読み込み処理は同期化（SynchronizedItemStreamReaderでラップ）する
			// （「FlatFileItemReader」がスレッドセーフではないため）
			.reader(step01ItemReader)

			// データの加工（あれば）
			// データの加工を行う場合はスレッドセーフにする必要があるので注意。
			//.processor(processor())

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			// （書き込みを行うクラス「MyBatisBatchItemWriter」はスレッドセーフのため同期は不要）
			.writer(step01ItemWriter)

			.build();
	}

	@Bean
	public ItemReader<Person> step01ItemReader(){
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
	public ItemWriter<Person> step01ItemWriter(){
	    return new MyBatisBatchItemWriterBuilder<Person>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.PersonMapper.insertPerson")
                .build();
	}

}
