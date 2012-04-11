mvn clean
mvn compile -DskipTests=true
mvn package
hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.Configurator
