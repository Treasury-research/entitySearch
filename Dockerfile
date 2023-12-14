from python:3.8.18
COPY . .
RUN pip3 install -r requirements.txt
CMD python3 main.py