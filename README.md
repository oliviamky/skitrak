# SkiTrak - Part One

## Project Overview

SkiTrak is a Java-based client-server system developed for Distributed Systems Assignment 1. The project simulates ski lift swipe events, sends concurrent HTTP POST requests to a server deployed on AWS EC2, and receives synthetic leaderboard updates through persistent WebSocket connections.

The main objective is to evaluate HTTP ingest throughput, request latency, concurrency, and WebSocket push latency under different client thread configurations.

## Running the Project
### 1. Deploy the Server

Build the WAR file
```bash
cd Server
mvn clean package
```

Upload the WAR file to EC2
```bash
scp -i <PEM_KEY_PATH> \
target/ski-servlet-1.0-SNAPSHOT.war \
ec2-user@<EC2_PUBLIC_IP>:/tmp/
```
Replace <PEM_KEY_PATH> with key path & <EC2_PUBLIC_IP> with EC2 public IP

Connect to the EC2 instance
```bash
ssh -i <PEM_KEY_PATH> ec2-user@<EC2_PUBLIC_IP>
```

Deploy the WAR file to Tomcat
Copy the uploaded WAR file into Tomcat's deployment directory, renaming it to ski-servlet.war:

```bash
sudo cp /tmp/ski-servlet-1.0-SNAPSHOT.war \
<TOMCAT_WEBAPPS_DIRECTORY>/ski-servlet.war
```

Deploy the generated WAR file to Apache Tomcat 9.

The expected application context is `/ski-servlet`.

Verify that the server is running:

```bash
curl http://<SERVER_IP>:8080/ski-servlet/health
```
Run the following commands in the Mac Terminal before executing Client Part 1 or Part 2

To ensure that clock synchronization completes successfully before running the benchmark
```bash
sudo sntp -sS time.apple.com
sntp time.apple.com
```
### 2. Run Client Part 1

Part 1 runs the multithreaded HTTP ingest workload and reports basic performance statistics.

```bash
cd ../client-part1
mvn clean compile
mvn exec:java -Dexec.mainClass=IngestPath -Dexec.args="<SERVER_IP> 300"
```

### 3. Run Client Part 2

Part 2 extends the client with detailed HTTP latency measurements, per-resort throughput, WebSocket push latency, and CSV exports.

```bash
cd ../client-part2
mvn clean compile
mvn exec:java -Dexec.mainClass=IngestPath -Dexec.args="<SERVER_IP> 300"
```

Replace `<SERVER_IP>` with the public IP address of the running EC2 instance.

The first argument specifies the server IP address, and the optional second argument specifies the number of sender threads for the main phase. The default is 300 threads. The selected thread count must divide 468,000 evenly.
