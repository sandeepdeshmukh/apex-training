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
  * Main monitor page 
  * Cluster Overview
  * DataTorrent Applications

2. Monitor an application
  * Logical
  * Physical
  * Physical DAG
  * Metrics
  * Attempts

### Change few parameters/properties to make it unique during the training

1. Change in Application.java @ApplicationAnnotation to dedup-your-name
1. Change in pom.xml artifactid to dedup-your-name

Compile and run again.

### Setup cluster environment for running the application
1. Setup SSH key
1. Login to remote cluster and create a personal folder
1. rsync using ssh your code within that folder 
  * rsync -avz -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"  ../myapexapp  training@X.X.X.X:/home/training/code/your-name
  * Exclude target folder
1. Compile and run the application on remote cluster
1. Access dtConsole and verify that the app is running successfully
