FROM bitnami/spark:latest

# Copie seus scripts ou arquivos necessários para o contêiner
COPY ./src /app
COPY ./data /app/data
COPY requirements.txt /app

# Defina o diretório de trabalho
WORKDIR /app

RUN pip install --upgrade pip

ADD requirements.txt .
RUN pip install -r requirements.txt

# Comando para iniciar o Spark
CMD ["spark-submit", "analysis.py"]