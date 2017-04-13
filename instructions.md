This is step-by-step guide for the Apex Trainer to conduct hands on sessions.

# Create template project

```shell
v="3.7.0-incubating"
mvn -B archetype:generate \
  -DarchetypeGroupId=org.apache.apex \
  -DarchetypeArtifactId=apex-app-archetype \
  -DarchetypeVersion="$v" \
  -DgroupId=com.example \
  -Dpackage=com.example.myapexapp \
  -DartifactId=myapexapp \
  -Dversion=1.0-SNAPSHOT
```
