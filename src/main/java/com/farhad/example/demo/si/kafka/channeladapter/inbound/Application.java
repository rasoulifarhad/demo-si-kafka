package com.farhad.example.demo.si.kafka.channeladapter.inbound;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.http.config.EnableIntegrationGraphController;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@IntegrationComponentScan
@EnableAutoConfiguration
@Configuration
@EnableIntegrationGraphController
@Slf4j
public class Application {

	public static void main(String[] args) throws Exception {

        log.info("");

		SpringApplication app = new  SpringApplication(Application.class) ;

		Map<String, Object> defaultProperties = new HashMap<>();
		defaultProperties.put("spring.config.name", "outbound");
		defaultProperties.put("spring.config.location", "optional:classpath:com/farhad/example/demo/si/kafka/channeladapter/outbound/");
        app.setDefaultProperties(defaultProperties);
		// app.setWebApplicationType(WebApplicationType.NONE);
		app.run(ArrayUtils.addAll(args
                        ,"--management.endpoints.web.exposure.include=*"));
		
    }

}