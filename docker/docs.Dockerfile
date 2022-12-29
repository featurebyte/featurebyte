FROM debian AS build

COPY ./ /tmp

# Delete unused directories
RUN find /tmp -mindepth 1 -maxdepth 1 -type d  | grep -vP '(\d+[.]\d+)|docs|latest' | xargs rm -rf

FROM nginx:latest

COPY --from=build /tmp/ /usr/share/nginx/html/
