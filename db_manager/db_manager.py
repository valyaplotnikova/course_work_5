import psycopg2

from src.config import config


class DBManager:

    @staticmethod
    def get_companies_and_vacancies_count():
        """
            получает список всех компаний и количество вакансий у каждой компании.
            """
        params = config()
        with psycopg2.connect(dbname='vacancies', **params) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT employers.employer_name, COUNT(vacancy_id) '
                            'from vacancies '
                            'INNER JOIN employers USING (employer_id)'
                            'GROUP BY employer_name')
                answer = cur.fetchall()
        conn.close()
        return answer

    @staticmethod
    def get_all_vacancies():
        """
        получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        """
        params = config()
        with psycopg2.connect(dbname='vacancies', **params) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT v.vacancy_name, e.employer_name, v.salary, v.vacancy_url '
                            'FROM vacancies AS v '
                            'INNER JOIN employers AS e USING (employer_id)')
                answer = cur.fetchall()
        conn.close()
        return answer

    @staticmethod
    def get_avg_salary():
        """
        получает среднюю зарплату по вакансиям.
        """
        params = config()
        with psycopg2.connect(dbname='vacancies', **params) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT AVG(salary)'
                            'FROM vacancies')
                answer = cur.fetchall()
        conn.close()
        return answer

    @staticmethod
    def get_vacancies_with_higher_salary():
        """
        получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям.
        """
        params = config()
        with psycopg2.connect(dbname='vacancies', **params) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT v.vacancy_name, e.employer_name, v.salary, v.vacancy_url '
                            'FROM vacancies AS v '
                            'INNER JOIN employers AS e USING (employer_id)'
                            'WHERE v.salary > (SELECT AVG(salary) FROM vacancies)')
                answer = cur.fetchall()
        conn.close()
        return answer

    @staticmethod
    def get_vacancies_with_keyword(keyword):
        """
        получает список всех вакансий,
        в названии которых содержатся переданные в метод слова,
        например python.
        """
        word = keyword
        params = config()
        with psycopg2.connect(dbname='vacancies', **params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT v.vacancy_name, e.employer_name, v.salary, v.vacancy_url "
                            f"FROM vacancies AS v "
                            f"INNER JOIN employers AS e USING (employer_id)"
                            f"WHERE vacancy_name LIKE '%{word}%'")
                answer = cur.fetchall()
        conn.close()
        return answer
