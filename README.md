# Люди XX века
Здесь хранятся материалы и код проекта «Люди XX века»

Проект «Люди XX века» – это агрегатор электронных баз, посвящённых истории России и СССР в XX веке и содержащих биографические данные. Такой агрегатор позволяет реализовать поиск людей сразу по нескольким базам. В ходе проекта разрабатывается общий формат машиночитаемого описания биографии, к которому можно привести данные из баз. 

Руководитель проекта – Даниил Скоринкин, участники: Илья Воронцов, Лилия Казакова, Артём Крюков, Мария Подрядчикова, Виктория Воробьёва, Анна Лёвина, Полина Янина, Ирина Махалова.

Основной репозиторий проекта: https://github.com/dhhse/20th_century_people \n
Репозиторий веб-приложения проекта: https://github.com/dhhse/biographies_webface

# О проекте
Проект «Люди XX века» – это агрегатор электронных баз, посвящённых истории России и СССР в XX веке и содержащих биографические данные.
Цель проекта – создать агрегатор для поиска людей сразу по нескольким электронным базам. В ходе проекта разрабатывается общий формат машиночитаемого описания биографии, к которому можно привести данные из баз. 

# Участники:
Даниил Скоринкин (доцент факультета гуманитарных наук НИУ ВШЭ)
Илья Воронцов
Лилия Казакова (магистратура «Компьютерная лингвистика» НИУ ВШЭ, выпуск 2023)
Артём Крюков (магистратура «Цифровые методы в гуманитарных науках» НИУ ВШЭ, выпуск 2021)
Мария Подрядчикова (магистратура «Компьютерная лингвистика» НИУ ВШЭ, выпуск 2020)
Виктория Воробьёва (магистратура «Цифровые методы в гуманитарных науках» НИУ ВШЭ, выпуск 2022)
Анна Лёвина (магистратура «Цифровые методы в гуманитарных науках» НИУ ВШЭ, выпуск 2022)
Полина Янина (магистратура «Цифровые методы в гуманитарных науках» НИУ ВШЭ, выпуск 2022)
Ирина Махалова (старший преподаватель факультета гуманитарных наук НИУ ВШЭ)

# Установка
Для работы потребуется установить Python 3 и docker.

Чтобы создать базу данных (Mongo DB) и очередь сообщений (RabbitMQ) в докере пропишите пароли
в файл `secrets.py` (шаблон есть в файле `secrets.py.template`), а также скопируйте эти же
пароли в `setup.sh` (шаблон файла лежит в `setup.sh.template`).

Запустите файл `setup.sh`. При этом будут скачаны и установлены Mongo и Rabbit, а также
будут настроены пользователи с нужными правами и созданы пустые базы данных.

Файл `secrets.py` потребуется, чтобы скрипты могли подключаться к базам данных и очередям сообщений.
