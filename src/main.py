from src.config import config
from src.utils import (get_employers_data, get_vacancies_data,
                       create_database, save_data_to_database_employers,
                       user_iterations, create_table)


def main():
    print("Получим данные о работодателях и их вакансиях")
    employers_id = [5339002, 1541784, 1171877, 62347, 5425152,
                    219425, 5925142, 3529, 1740, 1189354]

    params = config()

    vacancies_data = []
    employers_data = []
    for emp_id in employers_id:
        employers_data.append(get_employers_data(emp_id))
        vacancies_data.append(get_vacancies_data(emp_id))
    print("Данные получены")
    print("Создадим для работы базу данных и таблицы")
    create_database('vacancies', params)
    create_table('vacancies', params)
    print("Заполним таблицы полученными данными")
    save_data_to_database_employers(employers_data, vacancies_data, 'vacancies', params)
    user_iterations()


if __name__ == '__main__':
    main()
