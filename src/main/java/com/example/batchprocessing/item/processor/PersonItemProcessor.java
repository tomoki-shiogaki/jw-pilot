package com.example.batchprocessing.item.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.example.batchprocessing.dto.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

	private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

	private static int hoge = 0;

	@Override
	public Person process(final Person person) throws Exception {
		final String firstName = person.getFirstName().toUpperCase();
		final String lastName = person.getLastName();//.toUpperCase();

		Person transformedPerson = new Person(firstName, lastName);

		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		ppp();
		if(hoge == 9) {
			throw new Exception("check error at processor.");
		}

		return transformedPerson;
	}

	synchronized void ppp() {
		hoge++;
	}

}
