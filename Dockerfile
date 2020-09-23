FROM python:3.7
RUN adduser --disabled-password --gecos '' payal
EXPOSE 5000
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD python app.p
