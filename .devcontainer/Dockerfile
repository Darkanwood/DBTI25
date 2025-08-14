# Basis-Image: Ubuntu mit Java & Maven
FROM maven:3.9.6-eclipse-temurin-21

# Locale & System-Updates
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    mariadb-server

# MariaDB konfigurieren
#RUN service mysql start && \
#    mysql -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'password'; FLUSH PRIVILEGES;" && \
#    sed -i 's/^bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mariadb.conf.d/50-server.cnf

# SQL-Datei kopieren
COPY firma.sql /tmp/firma.sql

# MariaDB initialisieren & SQL-Skript einspielen
RUN service mariadb start && \
    sleep 5 && \
    mysql -u root < /tmp/firma.sql && \
    mysql -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'password'; FLUSH PRIVILEGES;"

# MariaDB beim Containerstart automatisch starten
COPY startup.sh /usr/local/bin/startup.sh

EXPOSE 3306
# ENTRYPOINT ["mariadbd-safe"]

# MongoDB installieren
RUN apt-get install -y gpg

RUN curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor && \
    echo "deb [ arch=amd64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
    >/etc/apt/sources.list.d/mongodb-org-7.0.list && \
    apt-get update && apt-get install -y mongodb-org

COPY mongod /etc/init.d/mongod 
RUN chmod 755 /etc/init.d/mongod

EXPOSE 27017

# cleanup
RUN rm -rf /var/lib/apt/lists/*