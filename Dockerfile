FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2025.2.99

ARG VBUILD=2025.2.0

ENV WEB_CERT=/run/secrets/web.cert \
    WEB_KEY=/run/secrets/web.key \
    WEB_PORT=1443

EXPOSE 80 443 444

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER/  && \
    python3 ./setup.py install

# UID fichiers = 984
# GID millegrilles = 980
USER 984:980

CMD ["-m", "web_scraper"]
