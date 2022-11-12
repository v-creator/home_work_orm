import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pprint

from models import create_tables, Publisher, Shop, Sale, Stock, Book

DSN = 'postgresql://postgres:postgres@localhost:5432/netology_db'
engine = sqlalchemy.create_engine(DSN)
Session = sessionmaker(bind=engine)
session = Session()


def add_data():
    publisher1 = Publisher(name='Пушкин')
    publisher2 = Publisher(name='Толстой')
    publisher3 = Publisher(name='Чехов')
    publisher4 = Publisher(name='Блок')

    session.add_all([publisher1, publisher2, publisher3, publisher4])
    session.commit()

    book1 = Book(title='Капитанская дочка', publisher=publisher1)
    book2 = Book(title='Руслан и Людмила', publisher=publisher1)
    book3 = Book(title='Евгений Онегин', publisher=publisher1)
    book4 = Book(title='Анна Каренина', publisher=publisher2)
    book5 = Book(title='Кавказский пленник', publisher=publisher2)
    book6 = Book(title='Вишневый сад', publisher=publisher3)
    book7 = Book(title='Человек в футляре', publisher=publisher3)
    book8 = Book(title='Три сестры', publisher=publisher3)
    book9 = Book(title='Двенадцать', publisher=publisher4)

    session.add_all([book1, book2, book3, book4, book5, book6, book7, book8, book9])
    session.commit()

    shop1 = Shop(name='Лабиринт')
    shop2 = Shop(name='Книжный дом')
    shop3 = Shop(name='Буквоед')

    session.add_all([shop1, shop2, shop3])
    session.commit()

    stock1 = Stock(book=book1, shop=shop3, count=100)
    stock2 = Stock(book=book2, shop=shop3, count=100)
    stock3 = Stock(book=book3, shop=shop3, count=100)
    stock4 = Stock(book=book1, shop=shop1, count=100)
    stock5 = Stock(book=book3, shop=shop2, count=100)
    stock6 = Stock(book=book2, shop=shop1, count=100)


    session.add_all([stock1, stock2, stock3, stock4, stock5, stock6])
    session.commit()

    sale1 = Sale(stock=stock1, price=600, data_sale='02-09-2022', count=10)
    sale2 = Sale(stock=stock2, price=500, data_sale='14-09-2022', count=5)
    sale3 = Sale(stock=stock3, price=450, data_sale='08-11-2022', count=7)
    sale4 = Sale(stock=stock4, price=600, data_sale='05-10-2022', count=3)
    sale5 = Sale(stock=stock5, price=600, data_sale='26-10-2022', count=3)



    session.add_all([sale1, sale2, sale3, sale4, sale5])
    session.commit()


def upload_data(id=None, name=None):
    result_dict = {}
    if id==None and name!=None:
        x1 = Publisher.name
        x2 = name
    elif id!=None and name==None:
        x1 = Publisher.id
        x2 = id
    books = session.query(Book).join(Publisher.book).filter(x1 == x2).subquery()
    stoks = session.query(Stock).join(books, Stock.id_book == books.c.id).subquery()
    sales_list = session.query(Sale).join(stoks, Sale.id_stock == stoks.c.id).all()
    for j in range(len(sales_list)):
        result_dict[j] = [sales_list[j].data_sale]
        result_dict[j].append(sales_list[j].price)


    sales_list_2 = session.query(Sale).join(stoks, Sale.id_stock == stoks.c.id).subquery()
    stock_list = session.query(Stock).join(sales_list_2, Stock.id == sales_list_2.c.id_stock).all()
    books2 = session.query(Book).join(Publisher.book).filter(x1 == x2).all()
    book_dict = {}
    for i in range(len(books2)):
        book_dict[books2[i].id] = books2[i].title
    for i in range(len(stock_list)):
        result_dict[i].append(book_dict[stock_list[i].id_book])


    for res in result_dict.items():
        print(f'{res[1][2]} | {res[1][1]} | {res[1][0]}')


if __name__ == '__main__':
    create_tables(engine)
    add_data()
    upload_data(id=1)
    # upload_data(name='Пушкин')
