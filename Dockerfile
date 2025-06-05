FROM python
COPY . /NewsApp
WORKDIR /NewsApp
RUN pip install -r requirements.txt
CMD ["python", "News.py"]