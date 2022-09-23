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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties(CdcCommonProperties.class)
public class CdcCommonConfiguration {

	private static final Log logger = LogFactory.getLog(CdcCommonConfiguration.class);

	@Bean
	public Properties cdcConfiguration(CdcCommonProperties properties) {
        Properties outProps = new java.util.Properties();
        outProps.putAll(properties.getConfig());
		return outProps;
	}

	@Bean
	@ConditionalOnMissingBean
    public Consumer<ChangeEvent<String, String>> defaultSourceRecordConsumer() {
		return sourceRecord -> logger.info("[CDC Event]: " + ((sourceRecord == null) ? "null" : sourceRecord.toString()));
	}

	@Bean
	public EmbeddedEngineExecutorService embeddedEngine(Consumer<ChangeEvent<String, String>> sourceRecordConsumer, java.util.Properties cdcConfiguration) {

        try {
            DebeziumEngine<ChangeEvent<String, String>> embeddedEngine = 
                    DebeziumEngine.create(io.debezium.engine.format.Json.class)
                            .using(cdcConfiguration)
                            .notifying(sourceRecordConsumer)
                            .build();

            return new EmbeddedEngineExecutorService(embeddedEngine) {
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

        } catch (Exception e) {
            return null;
        }
	}
}
