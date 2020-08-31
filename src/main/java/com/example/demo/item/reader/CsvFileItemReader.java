package com.example.demo.item.reader;

import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CsvFileItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements ResourceAwareItemReaderItemStream<T>, InitializingBean {

	private static final Logger log = LoggerFactory.getLogger(CsvFileItemReader.class);

    // default encoding for input files
    public static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private Resource resource;

    private boolean noInput = false;

    private int lineCount = 0;

    private Charset charset = DEFAULT_CHARSET;

    private int linesToSkip = 0;

    private boolean strict = true;

    private String lineSeparator = "\r\n";

    private char delimiter = ',';

    private char quote = '"';

    private String[] headers;

    private CsvParser csvParser;

    private FieldSetMapper<T> fieldSetMapper;

    public CsvFileItemReader() {
        setName(ClassUtils.getShortName(CsvFileItemReader.class));
    }

    /**
     * 読み込み対象のエンコーディングを設定します。デフォルトは {@link #DEFAULT_CHARSET}.
     *
     * @param charset 文字コード
     */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    /**
     * 最初に読み込みをスキップする行数を設定します
     *
     * @param linesToSkip the number of lines to skip
     */
    public void setLinesToSkip(int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    /**
     * strictModeを設定します
     *
     * @param strict <code>true</code> by default
     */
    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    /**
     * 1行の区切りとなる文字をセットします
     *
     * @param lineSeparator 区切り文字（CRLFの場合は\r\n, LFの場合は\n)
     */
    public void setLineSeparator(String lineSeparator){
        this.lineSeparator = lineSeparator;
    }

    /**
     * カラムの区切り文字をセットします
     *
     * @param delimiter 区切り文字
     */
    public void setDelimiter(char delimiter){
        this.delimiter = delimiter;
    }

    /**
     * カラムの囲み文字をセットします
     *
     * @param quote 囲み文字
     */
    public void setQuote(char quote){
        this.quote = quote;
    }

    /**
     * フィールドのヘッダ情報(Beanのフィールド名で表記)をセットします
     *
     * @param headers フィールドのヘッダ情報(Beanのフィールド名で表記)
     */
    public void setHeaders(String[] headers){
        this.headers = headers;
    }

    /**
     * フィールドへのMapperをセットします
     *
     * @param fieldSetMapper フィールドへ設定するためのMapper
     */
    public void setFieldSetMapper(FieldSetMapper<T> fieldSetMapper){
        this.fieldSetMapper = fieldSetMapper;
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    protected T doRead() throws Exception {
        if(noInput){
            return null;
        }

        String[] line = readLine();

        if(line == null){
            return null;
        }

        FieldSet fieldSet = new DefaultFieldSet(line, headers);
        return fieldSetMapper.mapFieldSet(fieldSet);
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(resource, "Input resource must be set");

        noInput = true;
        if(!resource.exists()){
            if(strict) {
                throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): " + resource);
            }
            log.warn("Input resource does not exist " + resource.getDescription());
            return;
        }

        if(!resource.isReadable()){
            if(strict){
                throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): " + resource);
            }
            log.warn("Input resource is not readable " + resource.getDescription());
        }

        csvParser = new CsvParser(settings());
        csvParser.beginParsing(new InputStreamReader(resource.getInputStream(), charset));
        for (int i = 0; i < linesToSkip; i++) {
            readLine();
        }

        noInput = false;
    }

    private CsvParserSettings settings(){
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator(lineSeparator);
        settings.getFormat().setDelimiter(delimiter);
        settings.getFormat().setQuote(quote);
        settings.setEmptyValue("");
        return settings;
    }

    private String[] readLine(){
        if(csvParser == null){
            throw new ReaderNotOpenException("Parser must be open before it can be read");
        }

        String[] line = csvParser.parseNext();
        if(line == null){
            return null;
        }
        lineCount++;

        return line;
    }


    @Override
    protected void doClose() throws Exception {
        lineCount = 0;
        if(csvParser != null){
            csvParser.stopParsing();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(headers, "headers is required");
        Assert.notNull(fieldSetMapper, "FieldSetMapper is required");
    }
}