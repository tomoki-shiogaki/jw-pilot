package com.example.batchprocessing;

import java.util.HashMap;
import java.util.function.Function;

import org.apache.ibatis.session.SqlSessionFactory;
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
import org.springframework.batch.item.ItemStreamReader;
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

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public SqlSessionFactory sqlSessionFactory;


	/**
	 * ジョブ定義のサンプルコード
	 *
	 * 下記一連の処理を実行するためのジョブを定義する
	 *
	 * 　　①CSV⇒DBインポート
	 * 　　②DB⇒（変換処理）⇒DB
	 * 　　③DB⇒CSVエクスポート
	 *
	 * @param listener
	 * @return
	 */
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())

			// リスナーを登録
			// ジョブの実行時や終了時などに実行される
			// （サンプルではジョブ終了時にPersonテーブルの内容をログに出力している）
			.listener(listener)

			// ①CSV⇒DBインポート
			.flow(step01_CSV_to_DB())

			// ②DB⇒（変換処理）⇒DB
			.next(flow02_DB_to_DB())

			// ③DB⇒CSVエクスポート
			.next(step03_DB_to_CSV())

			.end()
			.build();
	}

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
	public Step step01_CSV_to_DB() {
		Function<ItemStreamReader<Person>, SynchronizedItemStreamReader<Person>> wrapSynchronizedItemStreamReader = (itemReader) -> {
			SynchronizedItemStreamReader<Person> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate(itemReader);
			return synchronizedReader;
		};

		return stepBuilderFactory
			// ステップ名？(ログ出力や実行情報などを識別するために使用される？)
			.get("step01_CSV_to_DB")

			// チャンクサイズの設定
			// この単位でDBにコミットされる
			// （チャンクサイズ4、データ総件数10の場合、コミット回数は3回）
			.<Person, Person> chunk(2)

			// データの入力（CSV ⇒ DTO）
			// CSVファイルの読み込みを行うクラス「FlatFileItemReader」を使用してCSVファイルの各レコードをDTO「Person」に変換
			// 読み込み処理は同期化（SynchronizedItemStreamReaderでラップ）する
			// （「FlatFileItemReader」がスレッドセーフではないため）
			.reader(wrapSynchronizedItemStreamReader.apply(
					new FlatFileItemReaderBuilder<Person>()
					.name("personItemReader")
					.resource(new ClassPathResource("sample-data.csv"))
					.delimited()
					.names(new String[]{"firstName", "lastName"})
					.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
						setTargetType(Person.class);
					}})
					.build()
				))

			// データの加工（あれば）
			// データの加工を行う場合はスレッドセーフにする必要があるので注意。
			//.processor(processor())

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			// （書き込みを行うクラス「MyBatisBatchItemWriter」はスレッドセーフのため同期は不要）
			.writer(new MyBatisBatchItemWriterBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .statementId("com.example.batchprocessing.PersonMapper.insertPerson")
	                .build())

			// 非同期用のTaskExecutorを設定（デフォルト：SyncTaskExecutor）
			.taskExecutor(asyncTaskExecutor())

			// スレッドプールの最大数を設定（デフォルト：4）
			// スレッドはチャンク単位で割り当てられる
			// （スレッドプールの最大数10、チャンクサイズ4、データ総件数10の場合、使用されるスレッドは3）
			// （スレッドプールの最大数10、チャンクサイズ4、データ総件数60の場合、使用されるスレッドは10）
			//.throttleLimit(10)

			.build();
	}

	/**
	 * DBのレコードを加工するためのサンプルコード（DB ⇒ (加工) ⇒ DB）
	 *
	 * 「並行ステップ」を利用して加工処理を並列化。
	 * ただし、CSVファイルの読み込みを行うクラス「FlatFileItemReader」がスレッドセーフではないためデータの入力（ItemReader）は同期させている。
	 * そのため、並列化の効果が期待できるのはデータの加工（ItemProcessor）と出力（ItemWriter）のみ。
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
	public Flow flow02_DB_to_DB() {
	    return new FlowBuilder<SimpleFlow>("splitFlow02_DB_to_DB")
	    		// 非同期用のTaskExecutorを設定
	            .split(asyncTaskExecutor())

	            // 並行して実行するステップを登録
	            .add(
	            		// 名前に"J"が含まれるレコードを処理対象とするステップ
	            		new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_01").start(step02_DB_to_DB("step02_DB_to_DB_01", "J")).build(),

	            		// 名前に"Z"が含まれるレコードを処理対象とするステップ
	            		new FlowBuilder<SimpleFlow>("flow02_DB_to_DB_02").start(step02_DB_to_DB("step02_DB_to_DB_02", "Z")).build())

	            .build();
	}

	@Bean
	public Step step02_DB_to_DB(String stepName, String targetName) {
		return stepBuilderFactory.get(stepName)
			// チャンクサイズの設定
			.<Person, Person> chunk(3)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(new MyBatisCursorItemReaderBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .queryId("com.example.batchprocessing.PersonMapper.findPersonByName")
	                .parameterValues(new HashMap<String, Object>() {{put("name", targetName);}})
	                .build())

			// データの加工
			// ここにビジネスロジックを記述
			// （サンプルではPerson.firstNameを大文字に変換）
			.processor(new PersonItemProcessor())

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(new MyBatisBatchItemWriterBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .statementId("com.example.batchprocessing.PersonMapper.savePerson")
	                .build())

			.build();
	}

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
	public Step step03_DB_to_CSV() {
		return stepBuilderFactory.get("step03_DB_to_CSV")
			// チャンクサイズの設定
			.<Person, Person> chunk(2)

			// データの入力（DB ⇒ DTO）
			// DBのPersonテーブルの各レコードをDTO「Person」に変換
			.reader(new MyBatisCursorItemReaderBuilder<Person>()
	                .sqlSessionFactory(sqlSessionFactory)
	                .queryId("com.example.batchprocessing.PersonMapper.findAllPerson")
	                .build())

			// データの加工（あれば）
			//.processor(processor())

			// データの出力（DTO ⇒ CSV）
			// DTO「Person」をCSVに書き込む
			.writer(new FlatFileItemWriterBuilder<Person>()
           			.name("itemWriter")
           			.resource(new FileSystemResource("target/test-outputs/output.txt"))
           			//.headerCallback(new WriteHeaderFlatFileFooterCallback())
           			.lineSeparator("\r\n")
           			//.lineAggregator(new PassThroughLineAggregator<>())
           			.delimited()
    				.delimiter(",")
    				.names(new String[] {"firstName", "lastName"})
           			.build())

			.build();
	}

	@Bean
	public TaskExecutor asyncTaskExecutor(){
	    return new SimpleAsyncTaskExecutor("spring_batch");
		//return new SyncTaskExecutor();
	}

}
