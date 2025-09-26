# syntax=docker/dockerfile:1

ARG NODE_VERSION=20-alpine

FROM node:${NODE_VERSION} AS deps
WORKDIR /app

COPY package.json package-lock.json* pnpm-lock.yaml* yarn.lock* ./

RUN set -eux; \
    if [ -f package-lock.json ]; then \
        npm ci; \
    elif [ -f pnpm-lock.yaml ]; then \
        corepack enable; \
        pnpm install --frozen-lockfile; \
    elif [ -f yarn.lock ]; then \
        corepack enable; \
        yarn install --frozen-lockfile; \
    else \
        npm install; \
    fi

FROM node:${NODE_VERSION} AS build
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

ARG VITE_API_BASE_URL=http://localhost:8000
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}
ENV NEXT_TELEMETRY_DISABLED=1

RUN npm run build

FROM nginx:alpine AS runner

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
