FROM tensorflow/tensorflow:2.13.0-gpu as builder
WORKDIR /build
RUN apt-get update && apt-get install -y libpq-dev
COPY ./requirements.txt .
RUN pip install --user -r requirements.txt

FROM tensorflow/tensorflow:2.13.0-gpu
WORKDIR /nn
COPY --from=builder /root/.local /root/.local
COPY --from=builder /usr/lib/x86_64-linux-gnu/libpq.so.5 /usr/lib/x86_64-linux-gnu/libpq.so.5
ENV PATH=/root/.local/bin:$PATH
