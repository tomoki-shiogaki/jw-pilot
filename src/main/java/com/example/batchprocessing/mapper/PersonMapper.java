package com.example.batchprocessing.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.example.batchprocessing.dto.Person;

@Mapper
public interface PersonMapper {

    public List<Person> findAllPerson();

    public List<Person> findPersonByName();

    public Integer savePerson(Person person);

    public Integer insertPerson(Person person);

}
