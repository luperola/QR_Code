# Deploy su Heroku (Produzione)

## 1) Prerequisiti

- Heroku CLI installata
- Login: `heroku login`

## 2) Crea app e Postgres

```bash
heroku create <nome-app>
heroku addons:create heroku-postgresql:essential-0
```

Heroku imposta automaticamente `DATABASE_URL`.

## 3) Config vars consigliate

```bash
heroku config:set NODE_ENV=production
heroku config:set SESSION_KEY="Acmilan00"
heroku config:set BASE_URL="https://gtsqrcode.herokuapp.com"
```

## 4) Deploy

```bash
git push heroku main
heroku ps:scale web=1
```

## 5) Verifica

```bash
heroku open
heroku logs --tail
```

Endpoint di health:

- `GET /health`

## Note architetturali

- L'app usa PostgreSQL via `pg`.
- Le migrazioni base sono applicate all'avvio in `initDb()` (`CREATE TABLE IF NOT EXISTS` + indici + seed admin).
- Se `DATABASE_URL` non è presente, l'app termina con errore esplicito.
