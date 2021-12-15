FROM ubuntu:20.04
LABEL maintainer cezanne@codemayo.com

COPY run_featrest.sh /usr/bin/run_featrest.sh
COPY "requirements.txt" "featrest-1.0.0.tar.gz" "featuretools-0.23.3.tar.gz" /

RUN apt-get update && apt-get install -y --no-install-recommends \
    	    python3 python3-pip awscli && \
    pip3 install -r requirements.txt && \
    (tar xzvf featrest-1.0.0.tar.gz && cd featrest-1.0.0 && python3 setup.py install) && \
    (tar xzvf featuretools-0.23.3.tar.gz && cd featuretools-0.23.3 && python3 setup.py install) && \
    rm -rf /var/lib/apt/lists/* \
        requirements.txt featrest-1.0.0 featuretools-0.23.3 \
	featrest-1.0.0.tar.gz featuretools-0.23.3.tar.gz

CMD /usr/bin/run_featrest.sh
