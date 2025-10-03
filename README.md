# Data Vault 2.0 Project - dbt Implementation

## Описание проекта

Этот проект реализует архитектуру **Data Vault 2.0** с использованием **dbt** для загрузки данных из SQL Server источника в целевую базу данных. 

## Концепция Data Vault 2.0

**Data Vault 2.0** - это методология построения масштабируемых хранилищ данных. Основывается на принципах гибкости, масштабируемости и адаптивности к изменениям в бизнес-требованиях.

При создании проекта опиралась на концепцию Data Vault 2.0, реализовав архитектуру с Hub, Link и Satellite таблицами. 

**Техническая реализация:**
Для генерации суррогатных ключей написан макрос **generate_hash_key** с использованием MD5 хеширования
В Hub таблицы добавляем источник данных и натуральный ключ 
Для Satellite таблиц использован hash_diff (опционально) для отслеживания изменений атрибутов
Реализованы тесты качества данных и функционал документации через файлы schema.yml с описанием моделей, колонок, тестов и метаданных
Используются теги для группировки моделей по слоям (staging, hub, link, satellite, raw_vault)

**Возможные улучшения:**
Реализовать таблицу источников с ID для трассируемости данных
Добавить Business Vault слой для готовых к анализу данных
Расширить набор тестов для проверки связей между таблицами

<img width="781" height="842" alt="Диаграмма без названия drawio" src="https://github.com/user-attachments/assets/4171a321-dde4-40c9-a955-4e503f9e3875" />


## Архитектура Data Vault

Data Vault 2.0 состоит из трёх основных слоёв:

### **1. Staging Layer**
**Назначение**: Первичная загрузка и очистка данных из источников
- Загружает данные из источников
- Использует материализацию `table`

### **2. Raw Vault Layer**
**Назначение**: Хранение нормализованных данных в структуре Data Vault

#### **Hub**
**Назначение**: Хранят уникальные бизнес-ключи сущностей
- Содержат только бизнес-ключи (ID товара, ID магазина, ID поставщика)
- Генерируют суррогатные ключи через хеширование

**Пример**: `hub_goods` содержит `good_id` (бизнес-ключ) и `good_key` (суррогатный ключ)

#### **Link**
**Назначение**: Связывают Hub таблицы между собой
- Содержат составные ключи (хеш от всех связанных Hub)

**Пример**: `link_sale` связывает чек, магазин, товар и поставщика

#### **Satellite**
**Назначение**: Хранят описательные атрибуты и историю изменений
- Ссылаются на Hub или Link таблицы
- Содержат все описательные поля (названия, адреса, цены)
- Поддерживают историю изменений через `hash_diff`
- Отслеживают изменения атрибутов во времени

**Пример**: `sat_goods` содержит название товара, тип, ценовую группу

### **3. Business Vault**
**Назначение**: Не реализован, слой для данных готовой бизнес логики и вычислений

## Структура проекта

```
demo_dv_dbt/
├── models/
│   ├── staging/                    # Staging слой
│   │   ├── stg_dim_*.sql          # Staging таблицы для измерений
│   │   ├── stg_fact_*.sql         # Staging таблицы для фактов
│   │   └── sources.yml            # Определение источников данных
│   └── raw_vault/                 # Raw Vault слой
│       ├── hubs/                  # Hub таблицы
│       │   ├── hub_*.sql          # Hub модели
│       │   └── schema.yml         # Документация Hub
│       ├── links/                 # Link таблицы
│       │   ├── link_*.sql         # Link модели
│       │   └── schema.yml         # Документация Link
│       └── satellites/            # Satellite таблицы
│           ├── sat_*.sql          # Satellite модели
│           └── schema.yml         # Документация Satellite
├── macros/                        # dbt макросы
│   ├── generate_hash_key.sql      # Генерация хеш-ключей
│   └── generate_schema_name.sql   # Кастомная генерация схем
├── dbt_project.yml               # Конфигурация проекта
└── profiles.yml                  # Настройки подключения к БД
```

## Быстрый старт

### 1. Предварительные требования

```bash
# Установка Python и создание виртуального окружения
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Установка dbt-sqlserver
pip install dbt-sqlserver
```

### 2. Настройка подключения

Создайте файл `profiles.yml` в папке `~/.dbt/`:

### 3. Проверка подключения

```bash
# Проверка конфигурации dbt
dbt debug

```
### 4. Загрузка Data Vault (по слоям)

#### **Этап 1: Подготовка**
```bash
# Полная очистка всех данных
dbt run --full-refresh
```

#### **Этап 2: Staging слой**
```bash
# Загрузка всех Staging таблиц
dbt run --select tag:staging

# Проверка Staging таблиц
dbt test --select tag:staging
```

#### **Этап 3: Hub таблицы (параллельно)**
```bash
# Загрузка всех Hub таблиц 
dbt run --select tag:hub

# Проверка Hub таблиц
dbt test --select tag:hub
```

#### **Этап 4: Link и Satellite таблицы (параллельно)**
```bash
# Загрузка всех Link таблиц 
dbt run --select tag:link

# Загрузка всех Satellite таблиц 
dbt run --select tag:satellite

# Проверка Link таблиц
dbt test --select tag:link

# Проверка Satellite таблиц
dbt test --select tag:satellite
```

#### **Этап 5: Финальная проверка**
```bash
# Проверка всех тестов Data Vault
dbt test --select tag:raw_vault


## Созданные таблицы

### **Staging слой (6 таблиц)**
- `stg_dim_goodgroups` - Группы товаров
- `stg_dim_goods` - Товары
- `stg_fact_cheques` - Чеки
- `stg_dim_suppliers` - Поставщики
- `stg_dim_stores` - Магазины
- `stg_dim_manufacturers` - Производители

### **Hub таблицы (6 таблиц)**
- `hub_goodgroups` - Бизнес-ключи групп товаров
- `hub_goods` - Бизнес-ключи товаров
- `hub_cheques` - Бизнес-ключи чеков
- `hub_suppliers` - Бизнес-ключи поставщиков
- `hub_stores` - Бизнес-ключи магазинов
- `hub_manufacturers` - Бизнес-ключи производителей

### **Link таблицы (4 таблицы)**
- `link_sale` - Связь: чек ↔ магазин ↔ товар ↔ поставщик
- `link_good_manufacturer` - Связь: товар ↔ производитель
- `link_good_goodgroup` - Связь: товар ↔ группа товаров
- `link_goodgroup_hierarchy` - Иерархия групп товаров

### **Satellite таблицы (6 таблиц)**
- `sat_suppliers` - Атрибуты поставщиков
- `sat_stores` - Атрибуты магазинов
- `sat_manufacturers` - Атрибуты производителей
- `sat_goods` - Атрибуты товаров
- `sat_goodgroups` - Атрибуты групп товаров
- `sat_sales` - Атрибуты продаж

## Ключевые особенности реализации

### **Генерация суррогатных ключей**
```sql
-- Формула: HASH(business_key || record_source)
{{ generate_hash_key('good_id', 'OrganicNevaVP') }}
→ HASHBYTES('MD5', CONCAT_WS('||', CAST(good_id AS NVARCHAR(MAX)), 'OrganicNevaVP'))
```

```bash
# Просмотр всех моделей
dbt list

# Просмотр моделей по тегам
dbt list --select tag:staging
dbt list --select tag:hub
dbt list --select tag:link
dbt list --select tag:satellite

# Запуск конкретной модели
dbt run --select hub_goods

```

