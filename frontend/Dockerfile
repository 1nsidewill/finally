FROM node:20-alpine

WORKDIR /app

# 1. 패키지 설치
COPY package*.json ./
RUN npm install -g serve pm2 && npm install

# 2. 앱 코드 복사 및 빌드
COPY . .

# 3. 환경변수 복사
COPY .env.prod .env

RUN npm run build

# 3. config.js (PM2 ecosystem 파일) 생성
# 참고: https://pm2.keymetrics.io/docs/usage/expose/
#RUN printf "module.exports = {\n\
#  apps: [\n\
#    {\n\
#      name: 'frontend',\n\
#      script: 'serve',\n\
#      args: '-s build -l 3000'\n\
#    }\n\
#  ]\n\
#};\n" > /app/config.js

# 4. 포트 및 실행
EXPOSE 3000
#CMD ["pm2-runtime", "config.js"]
CMD ["serve", "-s", "build", "-l", "3000"]
