FROM node:lts-alpine3.19

WORKDIR /usr/src/app

COPY . .

COPY ./package*.json ./

RUN npm install

COPY . /usr/src/app

CMD ["npm", "start"]
