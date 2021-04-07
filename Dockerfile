FROM hseeberger/scala-sbt:8u222_1.3.5_2.13.1

COPY . /project
RUN apt update -y
RUN apt -qq upgrade -y 
RUN apt install maven
RUN cd /project && mvn package
CMD java -jar /project/target/BurstyTwitterStream-2.0.jar
