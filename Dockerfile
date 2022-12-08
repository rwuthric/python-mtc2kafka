# mtc2kafka Docker image build instructions
#

# ---------------------------------------------------------------------
# notes
# ---------------------------------------------------------------------
#
# Build the docker image:
#
#   docker build -t demofact/mtc2kafka .
#
# Run it in interactive mode
#
#   docker run -it --rm --init --name mtc2kafka -p5002:5000 demofact/mtc2kafka
#
# Run it in detached mode
#
#   docker run -d --name mtc2kafka demofact/mtc2kafka
#
# Stop it
#
#   docker stop mtc2kafka
#

FROM python:3.10
LABEL author="Rolf Wuthrich" description="Docker image for mtc2kafka"

RUN python3 -m pip install kafka-python
RUN python3 -m pip install requests

WORKDIR /opt/mtc2kafka
COPY . .