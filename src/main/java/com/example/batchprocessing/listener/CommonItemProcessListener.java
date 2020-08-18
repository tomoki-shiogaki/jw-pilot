package com.example.batchprocessing.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.stereotype.Component;

@Component
public class CommonItemProcessListener implements ItemProcessListener<Object, Object> {

    private static final Logger logger =
            LoggerFactory.getLogger(CommonItemProcessListener.class);

	@Override
	public void beforeProcess(Object item) {
		//logger.info("before item Process. [item:{}]", item);
	}

	@Override
	public void afterProcess(Object item, Object result) {
		//logger.info("after item Process. [item:{}]", item, result);
	}

    @Override
    public void onProcessError(Object item, Exception e) {
        logger.error("Exception occurred while processing. [item:{}]", item, e);
    }
}