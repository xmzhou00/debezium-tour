package com.xmzhou.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;

public class MySQLBinLogConnector {
    public static void main(String[] args) throws IOException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "root");
        client.setConnectTimeout(Integer.MAX_VALUE);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                System.out.println("===================");
                System.out.println(event);
            }
        });
        client.connect();
    }
}
