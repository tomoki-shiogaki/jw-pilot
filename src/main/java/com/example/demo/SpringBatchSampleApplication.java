package com.example.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class SpringBatchSampleApplication {

	// C:\pleiades\java\11\bin\java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=8000,suspend=n -jar "target/SpringBatchSample-0.0.1-SNAPSHOT.jar"
	//
	// どんなJavaアプリやjarライブラリでもデバックする方法(eclipseによるリモート・アタッチ)
	// https://web.plus-idea.net/2016/09/debug-java-remote-jvm/
	public static void main(String[] args) {
		SpringApplication.run(SpringBatchSampleApplication.class, args);
	}

    @RequestMapping("/runjob_ProcessBuilder/{jobname}")
    String requestJob_ProcessBuilder(@PathVariable("jobname") String jobname) throws URISyntaxException {

    	List<String> command = new ArrayList<>();

    	// javaコマンド
    	command.add(System.getProperty("java.home") + "\\bin\\java");

    	// java起動時オプション
    	command.add("-Dfile.encoding=UTF-8");

    	// データベース初期化はnever（行わない）を設定
    	command.add("-Dspring.datasource.initialization-mode=never");

		// ジョブ起動時にはTomcatは不要なのでnone（起動しない）を設定
		//
    	// Spring Bootで組み込みWebサーバの自動起動を無効化する方法
    	// https://reasonable-code.com/spring-boot-web-server-disable/
    	command.add("-Dspring.main.web-application-type=none");

    	// 実行するジョブを指定
    	command.add("-Dspring.batch.job.enabled=true");
    	command.add("-Dspring.batch.job.names="+jobname);

    	if(getApplicationScheme(SpringBatchSampleApplication.class).equals("jar")) {
    		// Jarを指定
    		command.add("-jar");
    		command.add(System.getProperty("java.class.path"));
    	}
    	else {
    		// classpathを指定
    		command.add("-classpath");
    		command.add(System.getProperty("java.class.path"));
    		command.add(SpringBatchSampleApplication.class.getName());
    	}

    	// ジョブパラメータ
		// （リトライする場合は同じパラメータを指定する）
    	command.add("run.id=1");

    	ProcessBuilder builder = new ProcessBuilder(command);

    	// 外部プログラムの入出力をJavaプロセスに統合する
    	//
    	// 外部プロセスの標準出力（や標準エラー）を読み出してやらないとバッファーが足りなくなって、
    	// 書き込み側（外部プロセス）がブロッキング（一時停止）される。（そのプロセスは終了できないことになる。）
    	// そのため、ProcessBuilderのinheritIO()メソッドで外部プログラムの入出力をJavaプロセスに統合し、
    	// バッファーがいっぱいにならないようにする。
    	//
    	// Javaから外部プログラム「7-zip」を呼び出す。
    	// https://qiita.com/nogitsune413/items/48d69054b75ea9afbe5b
    	builder.inheritIO();

    	try {
    		builder.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

    	return "run " + jobname;
    }

	private static String getApplicationScheme(Class<?> cls) throws URISyntaxException {
		ProtectionDomain pd = cls.getProtectionDomain();
		CodeSource cs = pd.getCodeSource();
		URL location = cs.getLocation();
		URI uri = location.toURI();
		return uri.getScheme();
	}

}
