FROM python:3.9
RUN apt -y install git
RUN python -m pip install --upgrade pip
RUN git clone https://github.com/openaq/openaq-fetch
WORKDIR /mnt
COPY requirements.txt .
RUN pip install --upgrade --ignore-installed --no-cache-dir -r requirements.txt
COPY . .