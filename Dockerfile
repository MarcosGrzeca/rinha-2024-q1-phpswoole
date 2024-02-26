FROM php:8.0-cli

WORKDIR /usr/src/app

COPY . .

#EXPOSE 9501

RUN composer install

COPY . /usr/src/app

CMD ["php", "index.php"]

