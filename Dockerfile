FROM python:3.12-slim

WORKDIR /app

# Copia solo i file necessari
COPY sinonimizzatore.py .
COPY morphit.db .
COPY pergamena.png .
COPY sfondo.png .
COPY index.html .

ENV HOST=0.0.0.0
EXPOSE 8080

CMD ["python", "sinonimizzatore.py", "--port", "8080"]
