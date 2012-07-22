REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/avro-1.5.4.jar
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/jackson-core-asl-1.7.3.jar
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/jackson-mapper-asl-1.7.3.jar
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/json-simple-1.1.jar
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/snappy-java-1.0.3.2.jar

fq1 = LOAD 'ContrailPlus/Flash_Mate_1_Avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
fq2 = LOAD 'ContrailPlus/Flash_Mate_2_Avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

fq1_filtered = FOREACH fq1 GENERATE $0, $1, $2;
fq2_filtered = FOREACH fq2 GENERATE $0, $1, $2;

joinedfq = JOIN fq1_filtered BY $0, fq2_filtered BY $0 ;

STORE joinedfq into 'ContrailPlus/Flash_Join_Out' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
	'{	"schema":{"type":"record","name":"joinedfqrecord",
 	"doc":"Foined FastQ File Structure",
	 "fields": [{ "name":"id1","type":"string"},
		  { "name":"read1", "type":"string"},
		  { "name":"qvalue1", "type":"string"},
		  { "name":"id2","type":"string"},
		  { "name":"read2", "type":"string"},
		  { "name":"qvalue2", "type":"string"}]}
		  }');
