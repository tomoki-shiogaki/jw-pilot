package com.example.demo.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.example.demo.entity.Person;
import com.example.demo.item.reader.CsvFileItemReader;

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
			// ステップ名
			.get("step01_CSV_to_DB")

			// チャンクサイズの設定
			// この単位でDBにコミットされる
			// （チャンクサイズ4、データ総件数10の場合、コミット回数は3回）
			.<Person, Person> chunk(4)

			// データの入力（CSV ⇒ DTO）
			.reader(step01ItemReader)

			// データの加工（あれば）
			//.processor(processor())

			// データの出力（DTO ⇒ DB）
			// DTO「Person」をDBのPersonテーブルに書き込む
			.writer(step01ItemWriter)

			.build();
	}

	@Bean
	public ItemReader<Person> step01ItemReader() {
		// SpringBatchで用意されているクラスだと改行含む文字列など対応できていないため、自作クラス「CsvFileItemReader」を使用する。
		// SpringBatchを実案件で活用するための10のプラクティス > 2. 区切り文字や改行を含むcsvを読み込む
		// https://qiita.com/nyasba/items/2dd9503edafc643fb786#2-%E5%8C%BA%E5%88%87%E3%82%8A%E6%96%87%E5%AD%97%E3%82%84%E6%94%B9%E8%A1%8C%E3%82%92%E5%90%AB%E3%82%80csv%E3%82%92%E8%AA%AD%E3%81%BF%E8%BE%BC%E3%82%80
		CsvFileItemReader<Person> reader = new CsvFileItemReader<>();

		// ItemReader名
		reader.setName("step01ItemReader");

		// ヘッダーの読み飛ばし（必要であれば）
		//reader.setLinesToSkip(1);

		// CSVファイル
		reader.setResource(new ClassPathResource("sample-data.csv"));

		// 改行コード
		reader.setLineSeparator("\r\n");


		// DTOとのマッピング
		// CSVデータの1列目がPerson.firstName、2列目がPerson.lastNameに格納される
		reader.setHeaders(new String[]{"firstName", "lastName"});
		reader.setFieldSetMapper(new FieldSetMapper<Person>() {
		    public Person mapFieldSet(FieldSet fs) {
		        if(fs == null){ return null; }
		        Person person = new Person();
		        person.setFirstName(fs.readString("firstName"));
		        person.setLastName(fs.readString("lastName"));
		        return person;
		    }
		});

		return reader;
	}

	@Bean
	public ItemWriter<Person> step01ItemWriter(){
	    return new MyBatisBatchItemWriterBuilder<Person>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.PersonMapper.insertPerson")
                .build();
	}

}
