/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logaritex.cdc.core;

import java.util.Properties;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties(CdcProperties.class)
public class CdcConfiguration {

	@Bean
	public Properties cdcConfiguration(CdcProperties properties) {
		Properties outProps = new java.util.Properties();
		outProps.putAll(properties.getConfig());
		return outProps;
	}

	@Bean
	public DebeziumEngine<?> debeziumEngineJson(Consumer<ChangeEvent<String, String>> changeEventConsumer,
			java.util.Properties cdcConfiguration) {

		DebeziumEngine<ChangeEvent<String, String>> debeziumEngine = DebeziumEngine
				.create(io.debezium.engine.format.Json.class)
				.using(cdcConfiguration)
				.notifying(changeEventConsumer)
				.build();

		return debeziumEngine;
	}

	// @Bean - TODO
	public DebeziumEngine<?> debeziumEngineAvro(Consumer<ChangeEvent<byte[], byte[]>> changeEventConsumer,
			java.util.Properties cdcConfiguration) {

		DebeziumEngine<ChangeEvent<byte[], byte[]>> debeziumEngine = DebeziumEngine
				.create(io.debezium.engine.format.Avro.class)
				.using(cdcConfiguration)
				.notifying(changeEventConsumer)
				.build();

		return debeziumEngine;
	}

	@Bean
	public EmbeddedEngineExecutorService embeddedEngine(DebeziumEngine<?> debeziumEngine) {

		return new EmbeddedEngineExecutorService(debeziumEngine) {
			@PostConstruct
			@Override
			public void start() {
				super.start();
			}

			@PreDestroy
			@Override
			public void close() {
				super.close();
			}
		};
	}
}
