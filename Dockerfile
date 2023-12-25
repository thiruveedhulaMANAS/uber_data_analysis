FROM  apache/airflow:latest

COPY requirements.txt .

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt

# add your pipeline folder path
# e.g -> COPY pipeline C:\uber-data-analysis\pipeline
COPY pipeline  replace_with_pipeline_file_path