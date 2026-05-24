# BigDataSpark

Лабораторная работа N2: ETL на Apache Spark.

Проект реализует обязательную часть задания и один опциональный пункт:

- PostgreSQL хранит исходную таблицу `staging.mock_data` и построенную Spark модель `dw`;
- Spark job `build_dw_postgres.py` перекладывает raw-данные в модель снежинки в PostgreSQL;
- ClickHouse хранит 6 обязательных витрин отчетности;
- MongoDB дополнительно хранит те же 6 витрин как опциональная NoSQL-база.

## Структура

```text
.
├── docker-compose.yml
├── README.md
├── исходные данные/
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   └── ...
├── sql/
│   ├── init/
│   │   ├── 00_create_schemas_and_staging.sql
│   │   └── 01_load_source_csv.sql
│   └── validation.sql
└── spark/
    ├── build_dw_postgres.py
    └── build_reports.py
```

## Запуск

Нужен Docker с Docker Compose.

```bash
docker compose up -d
```

При первом старте PostgreSQL автоматически создаст `staging.mock_data` и загрузит 10 CSV-файлов. В таблице должно быть `10000` строк.

Параметры подключения:

```text
PostgreSQL:
host: localhost
port: 5432
database: lab2
user: postgres
password: postgres

ClickHouse:
host: localhost
http port: 8123
native port: 9000
database: lab2
user: lab2
password: lab2

MongoDB:
mongodb://localhost:27017/lab2
```

## Spark jobs

Сначала построить модель данных в PostgreSQL:

```bash
docker compose exec spark spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/bitnami/spark/jobs/build_dw_postgres.py
```

Затем построить 6 отчетов в ClickHouse и MongoDB:

```bash
docker compose exec spark spark-submit \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /opt/bitnami/spark/jobs/build_reports.py
```

Первый запуск `spark-submit --packages` скачивает JDBC-коннекторы в volume `spark_ivy`, поэтому может занять несколько минут.

## Модель PostgreSQL

Схема `dw` создается Spark job-ом. Таблицы измерений:

- `dw.dim_country`
- `dw.dim_geo_location`
- `dw.dim_pet`
- `dw.dim_customer`
- `dw.dim_seller`
- `dw.dim_supplier`
- `dw.dim_store`
- `dw.dim_product_category`
- `dw.dim_product_attribute`
- `dw.dim_product`
- `dw.dim_date`

Факт:

- `dw.fact_sales`

Модель данных реализована как снежинка: центральная таблица фактов `dw.fact_sales` связана с измерениями, а часть измерений дополнительно нормализована в справочники географии, стран, категорий и атрибутов продуктов.

## Отчеты

Spark job `build_reports.py` создает 6 отдельных таблиц в ClickHouse и 6 одноименных коллекций в MongoDB:

- `report_product_sales`: продажи по продуктам, топ продуктов по количеству и выручке, выручка и количество по категориям, рейтинг и отзывы;
- `report_customer_sales`: продажи по клиентам, топ клиентов по сумме покупок, распределение клиентов и заказов по странам, средний чек;
- `report_time_sales`: месячные и годовые тренды, сравнение выручки с предыдущим месяцем, средний чек по месяцам;
- `report_store_sales`: продажи по магазинам, топ магазинов по выручке, распределение продаж по городам и странам, средний чек;
- `report_supplier_sales`: продажи по поставщикам, топ поставщиков по выручке, средняя цена товаров, распределение продаж по странам поставщиков;
- `report_product_quality`: продукты с наивысшим и наименьшим рейтингом, топ по отзывам, объем продаж и корреляция рейтинга с объемом продаж.

## Проверка

Проверить PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d lab2 -f /sql/validation.sql
```

Альтернативно можно выполнить содержимое `sql/validation.sql` в DBeaver.

Проверить ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user lab2 --password lab2 --query "
SELECT database, table, total_rows
FROM system.tables
WHERE database = 'lab2' AND table LIKE 'report_%'
ORDER BY table"
```

Проверить MongoDB:

```bash
docker compose exec mongo mongosh lab2 --eval "db.getCollectionNames().filter(n => n.startsWith('report_'))"
```

## Полезные команды

Открыть psql:

```bash
docker compose exec postgres psql -U postgres -d lab2
```

Открыть ClickHouse client:

```bash
docker compose exec clickhouse clickhouse-client --user lab2 --password lab2 -d lab2
```

Полностью пересоздать окружение:

```bash
docker compose down -v
docker compose up -d
```
