package com.xmzhou.mysql;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class MySQLSource {
    public static void main(String[] args) throws IOException {
        // Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "offset.dat");
        props.setProperty("offset.flush.interval.ms", "5000");
        /* begin connector properties */
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", "123456");
        props.setProperty("database.server.id", "85745");
        props.setProperty("database.include.list", "test");
        props.setProperty("table.include.list", "test.user");
        props.setProperty("topic.prefix", "my-app-connector");
        props.setProperty("schema.history.internal",
                "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename",
                "schemahistory.txt");

// Create the engine with this configuration ...
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(event ->{
                    System.out.println("===========================");
                    System.out.println(event);
                }).build()
        ) {
            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Do something else or wait for a signal or an event
        }
// Engine is stopped when the main code is finished
    }
}
