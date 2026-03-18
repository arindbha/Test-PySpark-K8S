FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fastapi_app/ fastapi_app/
COPY pyproject.toml .
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "fastapi_app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
