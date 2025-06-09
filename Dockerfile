FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


WORKDIR /app
COPY ./requirements.txt /app/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


COPY . /app/

EXPOSE 8000

# Rodar a API quando o container carregar
CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
