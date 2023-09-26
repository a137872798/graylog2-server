FROM graylog/graylog:5.1

RUN rm -f /usr/share/graylog/graylog.jar
COPY graylog.jar /usr/share/graylog/graylog.jar

