package com.example.batchprocessing.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.stereotype.Component;

@Component
public class CommonItemWriteListener implements ItemWriteListener<Object> {

    private static final Logger logger =
            LoggerFactory.getLogger(CommonItemWriteListener.class);

	@Override
	public void beforeWrite(List<? extends Object> items) {
		//logger.info("before item write. [items:{}]", items);
	}

	@Override
	public void afterWrite(List<? extends Object> items) {
		//logger.info("after item write. [items:{}]", items);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Object> items) {
		logger.error("Exception occurred while writing. [items:{}]", items, exception);
	}

}