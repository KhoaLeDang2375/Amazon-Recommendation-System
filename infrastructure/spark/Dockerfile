FROM python:3.11.11-slim-bookworm
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java
ENV JAVA_HOME=/opt/java
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --create-home --uid 1000 notebook \
    && mkdir -p /workspace/notebooks /workspace/outputs \
    && chown -R notebook:notebook /workspace
USER notebook
WORKDIR /workspace
EXPOSE 8888 4040
ENTRYPOINT ["tini", "--"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.root_dir=/workspace"]
