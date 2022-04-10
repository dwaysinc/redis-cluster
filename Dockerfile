FROM php:8.1-cli
COPY . /usr/src/example
WORKDIR /usr/src/example
CMD ["php", "./examples/get.php"]