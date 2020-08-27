package com.example.demo.item.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.example.demo.entity.Person;

public class PersonItemProcessor_ForErrorTest01 implements ItemProcessor<Person, Person> {

	private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor_ForErrorTest01.class);

	private int itemCount = 0;

	@Override
	public Person process(final Person person) throws Exception {
		final String firstName = person.getFirstName().toUpperCase();
		final String lastName = person.getLastName();

		Person transformedPerson = new Person(firstName, lastName);

		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		itemCount++;
		if (itemCount == 7) {
			throw new Exception("check error at processor.");
		}

		return transformedPerson;
	}

}
