# Финальный проект 1 семестра

REST API сервис для загрузки и выгрузки данных о ценах.

## Требования к системе

Система должна поддерживать go версии выше 1.23.3

## Установка и запуск

Как установить и запустить приложение локально?

1. Установить postgresql
```
sudo apt update
sudo apt install -y postgresql
```
3. Создать и настроить базу данных
```
psql -U postgres
CREATE USER validator WITH PASSWORD    'val1dat0r';
CREATE DATABASE "project-sem-1";
\c "project-sem-1"
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL
  );
```
4. Установить зависимости
```
bash ./scripts/prepare.sh
```
5. Запустить сервер
```
bash ./scripts/run.sh
```
6. Запустить тесты
```
bash ./scripts/tests.sh
```
## Тестирование

Директория `sample_data` - это пример директории, которая является разархивированной версией файла `sample_data.zip`


Отправить POST-запрос на запись в БД
```
curl -X POST -F "file=@sample_data.zip" http://localhost:8080/api/v0/prices
```
Отправить GET-запрос на скачивание записей из БД
```
curl -X GET -o response.zip http://localhost:8080/api/v0/prices
```
## Контакт

@DenisBorzenkov
