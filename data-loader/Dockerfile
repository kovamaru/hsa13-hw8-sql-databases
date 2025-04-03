FROM python:3.9

WORKDIR /app
COPY requirements.txt requirements.txt
RUN apt-get update && apt-get install -y default-mysql-client && \
    pip install --no-cache-dir -r requirements.txt
COPY . .

ENTRYPOINT ["sh", "/app/entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]