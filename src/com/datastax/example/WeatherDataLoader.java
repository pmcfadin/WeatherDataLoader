package com.datastax.example;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Properties;
import java.util.zip.GZIPInputStream;


/*

Copyright 2014 Patrick McFadin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
public class WeatherDataLoader {

    static private Session session;
    static private BoundStatement weatherDataInsert;
    static private final MetricRegistry metrics = new MetricRegistry();
    static private final Timer inserts = metrics.timer(MetricRegistry.name(Writer.class, "inserts"));

    public static void main(String[] args) {
        Path inputPath = FileSystems.getDefault().getPath("2005-us.csv.gz");

        long startTime = Calendar.getInstance().getTimeInMillis();

        loadInCassandra(inputPath);
        //sendToKafkaTopic(inputPath);

        long elapsedTimeInSeconds = (Calendar.getInstance().getTimeInMillis() - startTime) / 1000;

        System.out.println("Total time to load data: " + elapsedTimeInSeconds + " Seconds");
    }

    static void sendToKafkaTopic(Path inputPath) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("producer.type", "async");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<>(config);

        try {
            // Open a input stream to be uncompressed
            InputStream in = Files.newInputStream(inputPath);
            GZIPInputStream gis = new GZIPInputStream(in);

            // Use a buffered reader
            Reader decoder = new InputStreamReader(gis, Charset.defaultCharset());
            BufferedReader reader = new BufferedReader(decoder);

            // Read each line from the input file, transform and put into single file
            String msg;
            String[] lineValues;
            int lineNum = 0;

            while ((msg = reader.readLine()) != null) {
                lineValues = msg.split(",");
                String key = (lineValues[0] + lineValues[1] + lineValues[2] + lineValues[3] + lineValues[4]);

                KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("test", key, msg);

                // Time the insert
                final Timer.Context context = inserts.time();

                producer.send(keyedMessage);

                context.stop();

                lineNum++;
                if (lineNum % 1000 == 0) {
                    System.out.print("\rLine number: " + lineNum + "\tOps/s: " + inserts.getOneMinuteRate());
                }
            }

            in.close();

        } catch (Exception e) {
            System.out.println("Error while reading file: ");
            e.printStackTrace();

        }

        producer.close();
        System.out.println("complete");
    }

    static void loadInCassandra(Path inputPath) {
        Cluster cluster;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();

        session = cluster.connect("isd_weather_data");

        PreparedStatement weatherDataInsertStatement = session.prepare("INSERT INTO raw_weather_data (wsid, year, month, day, hour, temperature, dewpoint, pressure, wind_direction, wind_speed, sky_condition, one_hour_precip, six_hour_precip) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

        weatherDataInsert = new BoundStatement(weatherDataInsertStatement);

        try {
            // Open a input stream to be uncompressed
            InputStream in = Files.newInputStream(inputPath);
            GZIPInputStream gis = new GZIPInputStream(in);

            // Use a buffered reader
            Reader decoder = new InputStreamReader(gis, Charset.defaultCharset());
            BufferedReader reader = new BufferedReader(decoder);

            // Read each line from the input file, transform and put into single file
            String line;
            String[] lineValues;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineValues = line.split(",");

                // Time the insert
                final Timer.Context context = inserts.time();

                weatherDataInsert.setConsistencyLevel(ConsistencyLevel.QUORUM);
                session.executeAsync(weatherDataInsert.bind(lineValues[0], Integer.parseInt(lineValues[1]), Integer.parseInt(lineValues[2]), Integer.parseInt(lineValues[3]), Integer.parseInt(lineValues[4]), Double.parseDouble(lineValues[5]), Double.parseDouble(lineValues[6]), Double.parseDouble(lineValues[7]), Integer.parseInt(lineValues[8]), Double.parseDouble(lineValues[9]), Integer.parseInt(lineValues[10]), Double.parseDouble(lineValues[11]), Double.parseDouble(lineValues[12])));

                context.stop();

                lineNum++;
                if (lineNum % 1000 == 0) {
                    System.out.print("\rLine number: " + lineNum + "\tOps/s: " + inserts.getOneMinuteRate() + "\tp95 insert latency: " + inserts.getSnapshot().get95thPercentile() / 1000000);
                }
            }

            System.out.println("Load complete");
            System.out.println("Total records: " + lineNum);
            System.out.println("Ops/Sec (1 Minute rate): " + inserts.getOneMinuteRate());
            System.out.println("Ops/Sec (5 Minute rate): " + inserts.getFiveMinuteRate());
            System.out.println("Ops/Sec (15 Minute rate): " + inserts.getFifteenMinuteRate());
            System.out.format("95th percentile inserts: %.3f\n", inserts.getSnapshot().get95thPercentile() / 1000000);
            System.out.format("99th percentile inserts: %.3f\n", inserts.getSnapshot().get99thPercentile() / 1000000);

            in.close();

        } catch (Exception e) {
            System.out.println("Error while reading file: ");
            e.printStackTrace();

        }
        System.out.println();
    }

}
