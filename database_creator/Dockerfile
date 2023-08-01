FROM python:3.9 as builder
WORKDIR /build
COPY ./requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.9
WORKDIR /database_creator
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH