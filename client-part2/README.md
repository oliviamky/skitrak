

```bash
mvn clean compile
```

```bash
sudo sntp -sS time.apple.com   
sntp time.apple.com
```

```bash
mvn exec:java \
-Dexec.mainClass=IngestPath \
-Dexec.args="18.237.17.238"
```

```bash
mvn exec:java \
-Dexec.mainClass=IngestPath \
-Dexec.args="34.217.55.239 200"
```