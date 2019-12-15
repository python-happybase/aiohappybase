FROM aiudirog/aiohappybase-test-env:latest

COPY . .

RUN chmod +x entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
