import hashlib

import psycopg2
from psycopg2 import Error
from hashlib import sha256

def fetch_data(USER, PASS, HOST, PORT, DATABASE, QUERY):
    try:
        connection = psycopg2.connect(user=USER,
                                      password=PASS,
                                      host=HOST,
                                      port=PORT,
                                      database=DATABASE)
        cursor = connection.cursor()
        cursor.execute(QUERY)
        data = cursor.fetchall()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            return data
            print("Соединение с PostgreSQL закрыто")


def checkUsers(kc, hs):
    print("check start")
    li = []
    for i in kc:
        for j in hs:
            if i[0] == j[0]:
                li.append(i)
    print("check finished")
    return li

def  genQuery(id, fname, lname):
    dname = f"{fname} {lname[0]}."
    str = (
            f"update users set display_name = '{dname}' where id = '{id}';\n"
    )
    return str

def writeSQL(l):
    print("write start")
    with open("query.txt", "w", encoding="utf-8") as file:
        for rows in l:
            try:
                if rows[1] is not None and rows[2] is not None:
                    if rows[1] != '' and rows[2] == '':
                        file.write(genQuery(rows[0], rows[1], "Ф"))
                    elif rows[1] == '' and rows[2] == '':
                        file.write(genQuery(rows[0], "Имя", "Ф"))
                    elif rows[1] == '' and rows[2] != '':
                        file.write(genQuery(rows[0], rows[2][0], rows[2]))
                    elif rows[1] != '' and rows[2] != '':
                        file.write(genQuery(rows[0], rows[1], rows[2]))
                elif rows[1] is None and rows[2] is not None:
                    file.write(genQuery(rows[0], rows[2][0], rows[2]))
                elif rows[1] is not None and rows[2] is None:
                    file.write(genQuery(rows[0], rows[1], "Ф"))
                elif rows[1] is None and rows[2] is None:
                    file.write(genQuery(rows[0], "Имя", "Ф"))
            except(Exception, Error) as err:
                print("Ошибка при формировании sql-файла" + err)
                print("Проблемный юзер: " + rows)
    print("write finished")

def get_users_phone(user_ids):
    query = f'''
        select user_id, value
        from user_attribute ua 
        where name = 'phone' and id in ({user_ids});
    '''
    phones = fetch_data("postgres", "postgres", "localhost", "58494", "keycloak",  query)
    return phones

def main():
    print("Начали")

    ids = "'17aa8a98-57c9-41a0-9b1f-760eca3ac19e','35edfdef-b4bd-4598-ae70-f35161ff7d38','8d16f11f-e166-48ca-b76f-94b4e6ee163a','47ad38bf-6fca-4fce-b6bd-a6d237c2c0e4','083f6dad-ce3a-4719-a0e1-5b7d3e4cd7df','c5e7aa44-0d9f-4312-965c-41117d71f5f2','8031247f-c487-4a00-b2d3-f45fd427ba4b','95b832ad-d757-4013-afd1-a99b6e8a2d81','ae5d55a1-6a22-41fd-8e46-46bc3bcf49db','704e91c5-3ee4-46fd-95f3-8392063b7210','8ee76d4b-3243-4258-a128-76985a658261'"

    kc = get_users_phone(ids)

    ll = []

    for i in kc:
        if(i[1] is None or i[1] == ''):
            continue
        h = hashlib.new("sha256")
        h.update(i[1].encode())
        h = h.hexdigest()
        ll.append([i[0], h])

    print(ll)


def main_2():
    h_users_query = """
        select id, null
        from users u
        where deleted_at is null
        and display_name is null
    """

    k_users_query = """
        select id, first_name, last_name
        from user_entity ue
    """
    hs = fetch_data(
        "hs",
        "",
        "localhost",
        "54440",
        "postgres",
        h_users_query
    )

    kc = fetch_data(
        "kc",
        "",
        "localhost",
        "54440",
        "postgres",
        k_users_query
    )

    writeSQL(checkUsers(kc, hs))
    # writeSQL(list(filter(lambda i: i[0] in [x[0] for x in hs], kc)))

if __name__ == '__main__':
    main()