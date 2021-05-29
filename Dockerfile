FROM maven:3-openjdk-11

RUN mkdir /opt/ntw
WORKDIR /opt/ntw

COPY ./ /opt/ntw

RUN mvn clean install

ENTRYPOINT [ "mvn", "jetty:run"]