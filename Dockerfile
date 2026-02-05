FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry
COPY pyproject.toml poetry.lock README.md ./
COPY src/ ./src/
RUN poetry config virtualenvs.create false \
  && poetry install --only main --no-interaction --no-ansi

CMD ["poetry", "run", "python", "-m", "orchestrator.main"]