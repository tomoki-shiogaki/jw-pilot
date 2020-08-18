package com.example.batchprocessing.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.batchprocessing.listener.JobCompletionNotificationListener;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

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
	public Job importUserJob(
			JobCompletionNotificationListener listener,
			Step step01_CSV_to_DB,
			Flow flow02_DB_to_DB,
			Step step03_DB_to_CSV) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())

			// リスナーを登録
			// ジョブの実行時や終了時などに実行される
			// （サンプルではジョブ終了時にPersonテーブルの内容をログに出力している）
			.listener(listener)

			// ①CSV⇒DBインポート
			.flow(step01_CSV_to_DB)

			// ②DB⇒（変換処理）⇒DB
			.next(flow02_DB_to_DB)

			// ③DB⇒CSVエクスポート
			.next(step03_DB_to_CSV)

			.end()
			.build();
	}

}
