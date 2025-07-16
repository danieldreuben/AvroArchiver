package com.ross.serializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootApplication
@EnableScheduling
public class ArchiverApplication {

	//@Autowired
    //private OrderJobController controller;	

	@Autowired
	public static void main(String[] args) {
		SpringApplication.run(ArchiverApplication.class, args);
		new OrderJobController().run();
	}

}
