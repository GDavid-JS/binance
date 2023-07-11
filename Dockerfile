# FROM python:3.9
# WORKDIR /app
# COPY ./app/requirements.txt .
# RUN python3 -m pip install -r requirements.txt
# COPY ./app/main.py .
# CMD ["python3", "main.py"]
FROM python:3.9 as builder
WORKDIR /build
COPY ./app/requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.9
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY ./app/main.py .
ENV PATH=/root/.local/bin:$PATH
CMD ["python3", "main.py"]
