package com.example.demo.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.example.demo.entity.Person;

@Mapper
public interface PersonMapper {

	public List<Person> findAllPerson();

    public Integer insertPerson(Person person);

}
