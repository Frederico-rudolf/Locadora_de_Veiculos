FROM docker:27-cli AS docker-cli

FROM python:3.12-slim
# só o cliente docker e o plugin compose (o daemon é o do host, via socket montado)
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose
WORKDIR /app
RUN pip install --no-cache-dir "boto3>=1.35,<2"
COPY autoscaler.py .
EXPOSE 8080
CMD ["python", "-u", "autoscaler.py"]
