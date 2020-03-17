package com.example.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class TestProducer {
    public static void main(String[] args) {
        //default settings
        String[] params = new String[]{
                "/tmp/nyt201601/bigdata/NYT-yellow-201601", //directory - 0
                "15", //sleepTime - 1
                "kafka-to-ss", //topicName -2
                "1", //headerLength -3
                "0.0.0.0:6667"}; //bootstrapServers -4

        int i = 0;
        for (String arg : args) {
            params[i] = arg;
            i++;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", params[4]);
        // wprowadź poniżej pozostałe parametry producenta Kafki
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // uzupełnij polecenie tworzące producenta Kafki
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // przeanalizuj poniższy kod aby dowiedzieć się jak on działa
        final File folder = new File(params[0]);
        File[] listOfFiles = folder.listFiles();
        String listOfPaths[] = Arrays.stream(listOfFiles).
                map(File::getAbsolutePath).toArray(String[]::new);
        Arrays.sort(listOfPaths);
        for (final String fileName : listOfPaths) {
            try (Stream<String> stream = Files.lines(Paths.get(fileName)).
                    skip(Integer.parseInt(params[3]))) {
                // uzupełnij polecenie wysyłające komunikat do odpowiedniego
                // tematu Kafki. Do wskazania tematu użyj zmiennej params[2]
                // Kluczem niech będzie wyrażenie String.valueOf(line.hashCode())
                stream.forEach(line -> //System.out.println(line)
                        producer.send(new ProducerRecord<>(params[2], String.valueOf(line.hashCode()), line))
                );
                TimeUnit.SECONDS.sleep(Integer.parseInt(params[1]));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
