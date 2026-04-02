FROM python:3.12-slim

WORKDIR /app

# Copia solo i file necessari
COPY sinonimizzatore.py .
COPY morphit.db .
COPY pergamena.png .
COPY sfondo.png .

EXPOSE 8080

CMD ["python", "sinonimizzatore.py", "--port", "8080"]
