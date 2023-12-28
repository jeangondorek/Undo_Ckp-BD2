# Install

- If pip outdated

```bash
python -m pip install --upgrade pip
```

- Dependencies

```bash
pip install psycopg2
pip install configparser
```

### Docker

```bash
docker pull postgres
```

```bash
docker run --name tp2 -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

```bash
docker exec -it tp2 psql -U postgres
```



