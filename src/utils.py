from decimal import Decimal

import psycopg2
import requests

from db_manager.db_manager import DBManager


def get_employers_data(emp_id):
    """Получение данных о работодателях."""
    employer_id = emp_id
    url = f'https://api.hh.ru/employers/{employer_id}'
    headers = {'User-Agent': 'HH-User-Agent'}
    response = requests.get(url, headers=headers)
    return response.json()


def get_vacancies_data(emp_id):
    """Получение данных о вакансиях по работодателям."""
    url = 'https://api.hh.ru/vacancies'
    headers = {'User-Agent': 'HH-User-Agent'}
    params = {'text': '', 'page': 0, 'per_page': 100, 'employer_id': emp_id}
    response = requests.get(url, headers=headers, params=params)
    return response.json()['items']


def create_database(database_name, params):
    """Создание базы данных."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f'DROP DATABASE {database_name}')

    except psycopg2.errors.InvalidCatalogName:
        print('База данных не существует. Создадим Базу данных')

    cur.execute(f'CREATE DATABASE {database_name}')
    print("База данных создана")

    cur.close()
    conn.close()


def create_table(database_name, params):
    """Создание таблиц employers и vacancies в созданной базе данных - vacancies"""

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE IF NOT EXISTS employers (
                employer_id int primary key,
                employer_name varchar unique not null,
                url varchar(255)
             )
                """)
        print("Таблица employers создана")

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id serial primary key,
                    vacancy_name text not null,
                    salary int,
                    employer_id int not null,
                    vacancy_url varchar(255) not null
                    )
                    """)
        print("Таблица vacancies создана")

    with (conn.cursor() as cur):
        try:
            cur.execute("""alter table vacancies add constraint fk_employer_id 
            foreign key(employer_id) references employers(employer_id)""")
        except Exception:
            print('Таблица уже существует')

    conn.commit()
    conn.close()


def save_data_to_database_employers(employers_data, vacancies_data, database_name, params):
    """Заполнение таблиц employers и vacancies в созданной базе данных - vacancies"""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for emp in employers_data:
            cur.execute(
                """
                INSERT INTO employers (employer_id, employer_name, url) 
                VALUES (%s, %s, %s)
                """,
                (int(emp['id']), emp['name'], emp['alternate_url'])
            )

        for vac in vacancies_data:
            for v in vac:
                salary = v['salary']
                salary_from = 0
                if salary:
                    salary_from = v['salary']['from']
                    if salary_from is None:
                        salary_from = 0
                cur.execute(
                    """
                    INSERT INTO vacancies (vacancy_name, salary, employer_id,
                        vacancy_url)
                        VALUES (%s, %s, %s, %s)
                        """,
                    (v["name"], salary_from,
                        int(v['employer']['id']), v['alternate_url'])
                    )
    print("Таблицы заполнены")

    conn.commit()
    conn.close()


def user_iterations():
    while True:
        print("Выберете необходимое действие из списка:\n"
          "1 - получить список всех компаний и количество вакансий у каждой компании;\n"
          "2 - получить список всех вакансий с указанием названия компании, "
          "названия вакансии и зарплаты и ссылки на вакансию;\n"
          "3 - получить среднюю зарплату по вакансиям;\n"
          "4 - получить список всех вакансий;\n"
          "у которых зарплата выше средней по всем вакансиям;\n"
          "5 - получить список всех вакансий, "
          "в названии которых содержатся переданные в метод слова, "
          "например python;\n"
          "0 - выйти.")
        user_input = input()
        db = DBManager()
        if user_input == "1":
            answer = db.get_companies_and_vacancies_count()
            for ans in answer:
                print(ans, end='\n')
                print()
        elif user_input == "2":
            answer = db.get_all_vacancies()
            for ans in answer:
                print(ans, end='\n')
                print()
        elif user_input == "3":
            answer = db.get_avg_salary()
            print(f"Средняя заработная плата {int(answer[-1][-1])} рублей")
            print()
        elif user_input == "4":
            answer = db.get_vacancies_with_higher_salary()
            for ans in answer:
                print(ans, end='\n')
            print()
        elif user_input == "5":
            keyword = input("Введите слово для поиска ")
            answer = db.get_vacancies_with_keyword(keyword)
            if answer:
                for ans in answer:
                   print(ans, end='\n')
            else:
                print("По заданному запросу вакансий не найдено")
            print()
        elif user_input == "0":
            print('До свидания!')
            break


user_iterations()
