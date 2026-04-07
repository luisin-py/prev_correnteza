FROM python:3.11-slim

# Instala Chromium + Chromedriver
RUN apt-get update && \
    apt-get install -y --no-install-recommends chromium chromium-driver && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente
ENV CHROME_BINARY=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# Instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY . .

# ⚠️ Usa shell para interpretar a variável $PORT corretamente
CMD gunicorn -b 0.0.0.0:$PORT main:app
