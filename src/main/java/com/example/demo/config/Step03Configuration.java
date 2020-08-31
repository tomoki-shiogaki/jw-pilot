package com.example.demo.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.example.demo.entity.Person;
import com.example.demo.item.writer.CsvLineAggregator;

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
			.<Person, Person> chunk(4)

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
	    		.queryId("com.example.demo.mapper.PersonMapper.findAllPerson")
	    		.build();
	}

	@Bean
	public ItemWriter<Person> step03ItemWriter(){
	    return new FlatFileItemWriterBuilder<Person>()
	    		// ItemWriter名
       			.name("step03ItemWriter")

       			// ヘッダー（必要であれば）
       			//.headerCallback(writer -> writer.write("firstName,lastName"))

       			// CSVファイル
       			.resource(new FileSystemResource("bin/test-outputs/output.csv"))

       			// 改行コード
       			.lineSeparator("\r\n")

       			// SpringBatchで用意されているクラスだと囲み文字の設定が出来ないため、囲み文字に対応した自作クラス「CsvLineAggregator」を使用する。
       			// 【Spring Batch】テーブルデータをCSVファイルへ出力する
       			// https://qiita.com/teradatk/items/f860e582d5429dd81720
       			.lineAggregator(new CsvLineAggregator<Person>() {
       	            {
       	                setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
       	                    {
       	                    	// 出力対象のフィールドと順番
       	                        setNames(new String[] {"firstName", "lastName"});
       	                    }
       	                });
       	            }
       	        })

       			.build();
	}

}
