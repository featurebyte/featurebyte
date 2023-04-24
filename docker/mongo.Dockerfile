FROM mongo:6

# Replace the default entrypoint with our own
COPY entrypoint-mongo.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
