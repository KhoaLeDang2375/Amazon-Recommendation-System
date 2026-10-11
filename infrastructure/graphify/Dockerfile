FROM python:3.11.11-slim-bookworm
RUN pip install --no-cache-dir graphifyy==0.9.85
ENV GRAPHIFY_NO_AUTO_REFRESH=1
WORKDIR /workspace
ENTRYPOINT ["graphify"]
CMD ["--help"]
