import psycopg2
from psycopg2.sql import SQL, Identifier

with psycopg2.connect(database="clients", user="postgres", password="postgres") as conn:
    # Функция просмотра всей таблицы:
    def viewing_table(conn, name: str):
        with conn.cursor() as cur:
            query = "SELECT * FROM {};".format(name)
            cur.execute(query)
            return print(cur.fetchall())

    # Функция, создающая структуру БД (таблицы)
    def create_bd (conn):
        with conn.cursor() as cur:
            cur.execute("""
                    DROP TABLE phones;
                    DROP TABLE clients;
                    """)
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS clients(
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(60) NOT NULL,
                        last_name VARCHAR(80) NOT NULL,
                        email VARCHAR(80) UNIQUE NOT NULL
                        );
                    """)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS phones(
                        id SERIAL PRIMARY KEY,
                        phone BIGINT NOT NULL,
                        client_id INT NOT NULL REFERENCES clients(id)
                        );
                    """)
            conn.commit()
        pass
    # create_bd(conn)
    # viewing_table(conn, 'clients')
    # viewing_table(conn, 'phones')

    # Функция, позволяющая получить id клиента по почте
    def get_client_id(conn, email: str):
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT id FROM clients WHERE email=%s;
                        """, (email,))
            return cur.fetchone()[0]

    # Функция, позволяющая добавить телефон для существующего клиента
    def add_phone(conn, client_id, phone) -> int:
        with conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO phones(phone, client_id)
                    VALUES (%s, %s);
                    """, (phone, client_id))
        conn.commit()
        pass
    # viewing_table(conn, 'phones')
    # add_phone(conn, 1, 89998887766)
    # viewing_table(conn, 'phones')

    # Функция, позволяющая добавить нового клиента
    def add_client(conn, first_name, last_name, email, phone=None):
        with conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO clients(first_name, last_name, email)
                    VALUES (%s, %s, %s);
                    """, (first_name, last_name, email))
            conn.commit()
            if phone != None:
                client_id = get_client_id(conn, email)
                add_phone(conn, client_id, phone)
            else: pass
        pass
    # add_client(conn, 'Олег', 'Сидоров', 'oleg@yandex.com')
    # add_client(conn, 'Андрей', 'Петров', 'andrey@mail.ru')
    # add_client(conn, 'Маша', 'Сидорова', 'mariy@list.ru')
    # add_client(conn, 'Дарья', 'Хохлова', 'dasha@gmail.com', 85554553322)
    # add_phone(conn, 2, 88887776655)
    # add_phone(conn, 4, 86665554433)
    # add_phone(conn, 1, 85554443333)
    # add_phone(conn, 2, 84443332211)
    # viewing_table(conn, 'clients')
    # viewing_table(conn, 'phones')

    # Функция, позволяющая изменить данные о клиенте
    def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone=None):
        dic_data_client = {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone}
        with conn.cursor() as cur:
            dic_client_values = {}
            for key, value in dic_data_client.items():
                if value == None:
                    pass
                else:
                    dic_client_values[key] = value

            for key, value in dic_client_values.items():
                if key == 'phone':
                    cur.execute(("SELECT COUNT(phone) FROM phones WHERE client_id={}").format(client_id))
                    total_phones = cur.fetchone()[0]
                    if total_phones == 1:
                        cur.execute(("UPDATE phones SET phone={} WHERE client_id={}").format(phone, client_id))
                    else:
                        number = int(input('Какой номер изменить: '))
                        cur.execute(("UPDATE phones SET phone={} WHERE phone={}").format(phone, number))
                else:
                    cur.execute(SQL("UPDATE clients SET {}=%s WHERE id=%s").format(Identifier(key)), (value, client_id))

            if phone == None:
                cur.execute(("""
                        SELECT * FROM clients WHERE id={}
                        """).format(client_id))
            else:
                cur.execute(f"""SELECT first_name, last_name, email, p.phone FROM clients c
                left join phones p on p.client_id = c.id
                WHERE phone = {phone}""")
            return cur.fetchall()
    # print(change_client(conn, 3, 'Марья', None, None, None))
    # print(change_client(conn, 4, 'Дарья', None, None, 85554553355))
    # print(change_client(conn, 1, None, 'Иванов', 'ivanov@gmail.com', 89965554499))

    # Функция, позволяющая удалить телефон для существующего клиента
    def delete_phone(conn, client_id, phone) -> int:
        with conn.cursor() as cur:
            cur.execute("""
                    DELETE FROM phones
                    WHERE client_id = %s AND phone = %s;
                    """, (client_id, phone))
            conn.commit()
        pass
    # delete_phone(conn, 1, 85554443322)
    # viewing_table(conn, 'phones')

    # Функция, позволяющая удалить существующего клиента
    def delete_client(conn, client_id) -> int:
        with conn.cursor() as cur:
            cur.execute("""
                    SELECT COUNT(phone) FROM phones
                    WHERE client_id = %s;
                    """, (client_id, ))
            count_phones = cur.fetchone()[0]

            while count_phones > 0:
                cur.execute("""
                        DELETE FROM phones WHERE phone = (select phone from phones where client_id = %s limit 1);
                        """, (client_id,))
                conn.commit()
                count_phones -= 1

            cur.execute("""
                    DELETE FROM clients WHERE id=%s;
                    """, (client_id,))
            conn.commit()
        pass
    # delete_client(conn, 2)
    # viewing_table(conn, 'phones')
    # viewing_table(conn, 'clients')

    # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
    def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
        dic_data_client = {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone}
        with conn.cursor() as cur:
            dic_client_values = {}
            for key, value in dic_data_client.items():
                if value == None:
                    pass
                else:
                    dic_client_values[key] = value

            for key, value in dic_client_values.items():
                if key == 'phone':
                    cur.execute(f"""SELECT first_name, last_name, email, p.phone FROM clients c
                    left join phones p on p.client_id = c.id
                    WHERE phone = {phone}""")
                else:
                    cur.execute(SQL("SELECT * FROM clients WHERE {}=%s").format(Identifier(key)), (value, ))
            return cur.fetchall()
    # print(find_client(conn, 'Дарья', None, None, 85554553322))
    # print(find_client(conn, None, None, None, 85554553322))
    # print(find_client(conn, 'Маша', None, 'mariy@list.ru', None))

conn.close()