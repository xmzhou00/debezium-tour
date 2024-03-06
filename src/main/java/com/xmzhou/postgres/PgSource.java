package com.xmzhou.postgres;

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
public class PgSource {
    public static void main(String[] args) throws IOException {
        final Properties props = new Properties();
        props.setProperty("name", "PgTest");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        /* begin connector properties */
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5432");
        props.setProperty("database.user", "postgres");
        props.setProperty("database.password", "123456");
        props.setProperty("database.dbname", "postgres");
        // 逻辑复制插件
        props.setProperty("plugin.name","pgoutput");

        props.setProperty("slot.name","dbz_slot");
        props.setProperty("publication.name","pg_test_pub");
        // 当连接器正常停止时，是否删除slot
        props.setProperty("slot.drop.on.stop","true");



        props.setProperty("topic.prefix", "my-app-connector-pg");


        props.setProperty("schema.include.list", "public");
        props.setProperty("table.include.list", "public.user");

        props.setProperty("schema.history.internal",
                "io.debezium.storage.file.history.FileSchemaHistory");

        props.setProperty("schema.history.internal.file.filename",
                "schemahistory1.txt");

//        props.setProperty("database.connectionTimeZone","");
        props.setProperty("offset.flush.interval.ms", "5000");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "offset1.dat");

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

        }
    }

}
