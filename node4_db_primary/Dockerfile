FROM postgres:15

ENV POSTGRES_DB=pharmacy
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Enable replication
RUN echo "wal_level = replica" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 3" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "wal_keep_size = 64" >> /usr/share/postgresql/postgresql.conf.sample

COPY init.sql /docker-entrypoint-initdb.d/init.sql
