package com.kafkastreams.kafkaaapistream;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
	
	//Pipeline
	//Kstream y Ktable
	
	@Bean
	public KStream<String,String> pipeline(StreamsBuilder builder){
		
		//SOURCE:leer el topic mensajes-entrada
		KStream<String,String> ventas = builder.stream("ventas3");
		
		ventas.peek((key,value)->{
			System.out.println("Venta recibida" + key +":"+value);
		});
		
		KTable<Windowed<String>,Long> conteo = 
				ventas.groupByKey()
				.windowedBy(
						TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))).count();
		
		
		conteo.toStream().map((key,value)->
				KeyValue.pair(key.key(), "Eventos:" + value)
				).to("ventas-window3");
		
		return ventas;

		
	}
	
	

}
