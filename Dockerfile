FROM astrocrpublic.azurecr.io/runtime:3.3-2
# ENV JAVA_HOME=/usr/lib/jvm/default-java
# Chuyển sang quyền root để cài đặt
USER root

# Cài đặt Stockfish
RUN apt-get update && \
    apt-get install -y --no-install-recommends stockfish && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường Java (như đã bàn luận trước đó)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_SUBMIT_ARGS="--master local[*] pyspark-shell"

# Trả lại quyền cho user astro
USER astro