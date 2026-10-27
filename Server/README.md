
```bash
mvn clean package
```

```bash
scp -i [pem-location] \
target/ski-servlet-1.0-SNAPSHOT.war \
ec2-user@[ec2IPAddress]:/tmp/
```

```bash
sudo cp /tmp/ski-servlet-1.0-SNAPSHOT.war \
/opt/tomcat9/webapps/ski-servlet.war
```

```bash
curl --max-time 5 -i -X POST \
"http://localhost:8080/ski-servlet/skiers/1/seasons/2026/days/1/skiers/12345" \
-H "Content-Type: application/json" \
-d '{"time":217,"liftID":21}'
```

```bash
curl --max-time 5 -i http://localhost:8080/ski-servlet/health
```