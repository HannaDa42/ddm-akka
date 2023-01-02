# ddm-akka
## Use to build a .jar file of your project to spread and run it

Terminal - Program path
```sh
mvn package -f "pom.xml"
```


## Try to run the .jar file:

Terminal - Program path
```sh
java -jar target/ddm-akka-1.0.jar
```
Output: No parameters given

## Define Master
Start a master process [params - to setup the master]

e.g. with host
```sh
java -jar target/ddm-akka-1.0.jar master -h localhost
```
