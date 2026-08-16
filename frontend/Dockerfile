# Build the dashboard, then serve the static bundle from nginx.  nginx also
# proxies /api to the backend, so the browser talks to one origin.
FROM node:22-alpine AS build

WORKDIR /app

# npm ci installs exactly what package-lock.json pins, so an image built today
# and one built in six months contain the same dependency tree.
COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json vite.config.ts index.html ./
COPY public/ ./public/
COPY src/ ./src/
RUN npm run build


FROM nginx:1.27-alpine

# The image's entrypoint renders anything in templates/ before starting nginx,
# and populates NGINX_LOCAL_RESOLVERS from the container's resolv.conf when asked.
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1
COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
