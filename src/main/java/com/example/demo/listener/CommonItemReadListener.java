package com.example.demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

@Component
public class CommonItemReadListener implements ItemReadListener<Object> {

    private static final Logger logger =
            LoggerFactory.getLogger(CommonItemReadListener.class);

	@Override
	public void beforeRead() {
		//logger.info("before item read.");
	}

	@Override
	public void afterRead(Object item) {
		//logger.info("after item read. [item:{}]", item);
	}

    @Override
    public void onReadError(Exception ex) {
        logger.error("Exception occurred while reading.", ex);
    }
}
