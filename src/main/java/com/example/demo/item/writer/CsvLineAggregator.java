package com.example.demo.item.writer;

import org.springframework.batch.item.file.transform.ExtractorLineAggregator;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

public class CsvLineAggregator<T> extends ExtractorLineAggregator<T> {

    private CsvWriter writer;

    public CsvLineAggregator() {
        CsvWriterSettings settings = new CsvWriterSettings();
        writer = new CsvWriter(settings);
    }

    @Override
    protected String doAggregate(Object[] fields) {
        return writer.writeRowToString(fields);
    }

}