package com.example.batchprocessing.config;

import java.util.function.Function;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.example.batchprocessing.dto.Person;

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
	 * 「マルチスレッドステップ」を利用してインポート処理を並列化。
	 * ただし、CSVファイルの読み込みを行うクラス「FlatFileItemReader」がスレッドセーフではないためデータの入力（ItemReader）は同期させている。
	 * そのため、並列化の効果が期待できるのはデータの加工（ItemProcessor）と出力（ItemWriter）のみ。
	 *
	 * 　　<参考サイト>
	 *
	 * 　　Spring Batchのアーキテクチャ > Spring Batchの基本構造
	 * 　　https://terasoluna-batch.github.io/guideline/5.0.0.RELEASE/ja/Ch02_SpringBatchArchitecture.html#Ch02_SpringBatchArch_Overview_BasicStructure
	 * 　　
	 * 　　Spring Batch - リファレンスドキュメント > ItemReader および ItemWriter > FlatFileItemReader
	 * 　　https://spring.pleiades.io/spring-batch/docs/current/reference/html/readersAndWriters.html#flatFileItemReader
	 * 　　
	 * 　　Spring Batch - リファレンスドキュメント > スケーリングと並列処理 > マルチスレッドステップ
	 * 　　https://spring.pleiades.io/spring-batch/docs/current/reference/html/scalability.html#multithreadedStep
	 * 　　
	 * 　　spring-batchのTaskExecutorによるstepマルチスレッド化
	 * 　　https://kagamihoge.hatenablog.com/entry/2020/01/07/110847
	 *
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
			.<Person, Person> chunk(10)

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

			// 非同期用のTaskExecutorを設定（デフォルト：SyncTaskExecutor）
			.taskExecutor(asyncTaskExecutor())

			// スレッドプールの最大数を設定（デフォルト：4）
			// スレッドはチャンク単位で割り当てられる
			// （スレッドプールの最大数10、チャンクサイズ4、データ総件数10の場合、使用されるスレッドは3）
			// （スレッドプールの最大数10、チャンクサイズ4、データ総件数60の場合、使用されるスレッドは10）
			//.throttleLimit(10)

			.build();
	}

	@Bean
	public ItemReader<Person> step01ItemReader(){
		Function<ItemStreamReader<Person>, SynchronizedItemStreamReader<Person>> wrapSynchronizedItemStreamReader = (itemReader) -> {
			SynchronizedItemStreamReader<Person> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate(itemReader);
			return synchronizedReader;
		};

	    return
	    	wrapSynchronizedItemStreamReader.apply(
				new FlatFileItemReaderBuilder<Person>()
				.name("personItemReader")
				.resource(new ClassPathResource("sample-data.csv"))
				.delimited()
				.names(new String[]{"firstName", "lastName"})
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
					setTargetType(Person.class);
				}})
				.build()
			);
	}

	@Bean
	public ItemWriter<Person> step01ItemWriter(){
	    return new MyBatisBatchItemWriterBuilder<Person>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.batchprocessing.mapper.PersonMapper.insertPerson")
                .build();
	}

	public TaskExecutor asyncTaskExecutor(){
	    return new SimpleAsyncTaskExecutor("spring_batch");
		//return new SyncTaskExecutor();
	}

}
