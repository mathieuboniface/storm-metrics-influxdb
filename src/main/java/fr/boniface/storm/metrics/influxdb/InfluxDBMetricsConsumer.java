/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2016 Mathieu Boniface
 *
 */

package fr.boniface.storm.metrics.influxdb;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Collection;
import java.util.Map;

@Slf4j
public class InfluxDBMetricsConsumer implements IMetricsConsumer {

	private static final String INFLUXDB_URL = "metrics.influxdb.url";
    private static final String INFLUXDB_USERNAME = "metrics.influxdb.username";
    private static final String INFLUXDB_PASSWORD = "metrics.influxdb.password";
    private static final String INFLUXDB_DATABASE = "metrics.influxdb.db";
    private static final String INFLUXDB_MEASUREMENT_PREFIX = "metrics.influxdb.measurement.prefix";

	private String influxdbURL;
    private String influxdbUsername;
    private String influxdbPassword;
	private String influxdbDatabase;
    private String influxdbMeasurementPrefix = "";

    private String topologyName;

    private InfluxDB influxDB = null;

	@Override
	public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		configure(stormConf);

		if (registrationArgument instanceof Map) {
			configure((Map) registrationArgument);
		}

        connectIfNeeded();
	}

    private void connectIfNeeded() {
        if (influxDB == null) {
            log.debug("Creating new connection to influxdb : [url='{}' username='{}' database='{}]", influxdbURL, influxdbUsername, influxdbDatabase);
            influxDB = InfluxDBFactory.connect(influxdbURL, influxdbUsername, influxdbPassword);
        }
    }

    void configure(@SuppressWarnings("rawtypes") Map conf) {
		if (conf.containsKey(Config.TOPOLOGY_NAME)) {
			topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		}
		if (conf.containsKey(INFLUXDB_URL)) {
			influxdbURL = (String) conf.get(INFLUXDB_URL);
		}
        if (conf.containsKey(INFLUXDB_USERNAME)) {
            influxdbUsername = (String) conf.get(INFLUXDB_USERNAME);
        }
        if (conf.containsKey(INFLUXDB_PASSWORD)) {
            influxdbPassword = (String) conf.get(INFLUXDB_PASSWORD);
        }
        if (conf.containsKey(INFLUXDB_DATABASE)) {
            influxdbDatabase = (String) conf.get(INFLUXDB_DATABASE);
        }
        if (conf.containsKey(INFLUXDB_MEASUREMENT_PREFIX)) {
            influxdbMeasurementPrefix = (String) conf.get(INFLUXDB_MEASUREMENT_PREFIX);
        }

	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        connectIfNeeded();

        BatchPoints batchPoints = BatchPoints
                .database(influxdbDatabase)
                .tag("worker", taskInfo.srcWorkerHost)
                .tag("workerPort", taskInfo.srcWorkerPort + "")
                .tag("componentId", taskInfo.srcComponentId)
                .tag("taskId", taskInfo.srcTaskId + "")
                .tag("topology", topologyName)
                .build();

		for (DataPoint dataPoint : dataPoints) {

            String name = installMeasurementPrefix(dataPoint.name);
            Object value = dataPoint.value;

            recordDatapoint(name, value, batchPoints);
        }

        influxDB.write(batchPoints);
	}

    private String installMeasurementPrefix(String name) {
        return influxdbMeasurementPrefix + name;
    }

    private void recordDatapoint(String name, Object value, BatchPoints batchPoints) {
        if (value == null) {
            log.warn("Datapoint will not be exported. Value is null : [name:'{}' value:'{}']", name, value);

        } else if (Boolean.class.isAssignableFrom(value.getClass())) {

            Point point = Point.measurement(name)
                    .addField("value", (Boolean) value)
                    .build();

            batchPoints.point(point);

        } else if (Long.class.isAssignableFrom(value.getClass())) {

            Point point = Point.measurement((name)).
                    addField("value", (Long) value)
                    .build();

            batchPoints.point(point);

        } else if (Double.class.isAssignableFrom(value.getClass())) {

            Point point = Point.measurement(name)
                    .addField("value", (Double) value)
                    .build();

            batchPoints.point(point);

        } else if (Number.class.isAssignableFrom(value.getClass())) {

            Point point = Point.measurement(name)
                    .addField("value", (Number) value)
                    .build();

            batchPoints.point(point);

        } else if (String.class.isAssignableFrom(value.getClass())) {

            Point point = Point.measurement(name)
                    .addField("value", (String) value)
                    .build();

            batchPoints.point(point);

        } else if (Map.class.isAssignableFrom(value.getClass())) {

            Map<String, Object> values = (Map<String, Object>) value;
            for (Map.Entry entry : values.entrySet()) {
                recordDatapoint(name + "." + entry.getKey(), entry.getValue(), batchPoints);
            }

        } else {
            log.warn("Datapoint will not be exported. Unable to determine the Java type of 'value' : [value:'{}' type:'{}']", name, value.getClass().getSimpleName());
        }

    }

    @Override
	public void cleanup() {
		influxDB = null;
	}

}
