This is step-by-step guide for the Apex Trainer to conduct hands on sessions.

# Project creation using archetype

### Create project
```shell
v="3.5.0"
mvn -B archetype:generate \
  -DarchetypeGroupId=org.apache.apex \
  -DarchetypeArtifactId=apex-app-archetype \
  -DarchetypeVersion="$v" \
  -DgroupId=com.example \
  -Dpackage=com.example.myapexapp \
  -DartifactId=myapexapp \
  -Dversion=1.0-SNAPSHOT
```

### Compile and verify 
```bash
mvn clean package -DskipTests
```

### Folder structure walkthrough
1. Walk through the folders and code
..1. src, test, resources/META-INF/properties.xml, pom.xml - dependencies

### Run and show the DAG 
```bash
apex
launch target/myapexapp-1.0-SNAPSHOT.apa
```

### Go through the DTConsole
1. Run through the monitor console
..1. Main monitor page 
...1. Cluster Overview
...2. DataTorrent Applications

2. Monitor an application - Logical, Physical, Physical DAG, Metrics, Attempts.


