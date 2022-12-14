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

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 *
 * @author Christian Tzolov
 */
@ConfigurationProperties("cdc")
@Validated
public class CdcProperties {

	/**
	 * Spring pass-trough wrapper for debezium configuration properties. All properties with a 'cdc.debezium.' prefix are
	 * native Debezium properties. The prefix is removed, converting them into Debezium
	 * io.debezium.config.Configuration.
	 */
	private Map<String, String> debezium = defaultConfig();

	public Map<String, String> getDebezium() {
		return debezium;
	}

	private Map<String, String> defaultConfig() {
		Map<String, String> defaultConfig = new HashMap<>();
		defaultConfig.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
		// defaultConfig.put("offset.flush.interval.ms", "60000");
		return defaultConfig;
	}
}
