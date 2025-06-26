package com.ross.excel.serializer;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootApplication
public class ArchiverApplication {
		

	@Autowired
    //private AvroAchiver archiverApp;	

	public static void main(String[] args) {
		SpringApplication.run(ArchiverApplication.class, args);
	}

}
