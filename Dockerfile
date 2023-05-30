FROM python:3.9.16-slim

RUN sed -i 's#http://deb.debian.org#https://mirrors.ustc.edu.cn#g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|mirrors.ustc.edu.cn/debian-security|g' /etc/apt/sources.list

RUN apt-get update && apt-get install -y proxychains4 gcc g++

RUN echo "http 127.0.0.1 7890" >> /etc/proxychains.conf

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.doubanio.com/simple

CMD ["cat", "/etc/proxychains.conf"]

CMD ["proxychains4", "python", "scripts/stream-orderbook.py"]
