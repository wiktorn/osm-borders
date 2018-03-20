#!/bin/sh

rm -f package_fetcher.zip
rm -f package_rest.zip

docker run --rm -v $PWD:/input lambci/lambda:build-python3.6 /bin/bash -c "mkdir /output && \
	grep -v boto /input/requirements.txt > /tmp/requirements.txt && \
	pip3.6 install -t /output -r /tmp/requirements.txt && \
	cp -r /input/{borders,converters,templates,rest_server.py} /output && \
	cp /input/amazon/flask_lambda.py /input/amazon/fetcher.py /output && \
	cd /output && \
	zip -Xr /input/package_fetcher.zip ."

docker run --rm -v $PWD:/input lambci/lambda:build-python3.6 /bin/bash -c "mkdir /output && \
	grep -v boto /input/requirements-rest.txt > /tmp/requirements.txt && \
	pip3.6 install -t /output -r /tmp/requirements.txt && \
	cp -r /input/{converters,templates,rest_server.py} /output && \
	cp /input/amazon/flask_lambda.py /input/amazon/rest_endpoint.py /output && \
	cd /output && \
	zip -Xr /input/package_rest.zip ."

