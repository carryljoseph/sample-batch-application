CURRENT_DATE=`date '+%Y/%m/%d'`
LESSON=$(basename $PWD)
mvn clean package -Dmaven.test.skip=true;
java -javaagent:C:\\Users\\HP\\Documents\\datadog\\dd-java-agent-0.8.0.jar -Ddd.logs.injection=true -Ddd.trace.sample.rate=1 -Ddd.service=billpaybatch -Ddd.env=test -jar ./target/linkedin-batch-0.0.1-SNAPSHOT.jar "run.date(date)=$CURRENT_DATE" "lesson=$LESSON";
read;
