FROM scratch

CMD ["/app"]

ADD infrastructure/schema/ infrastructure/schema/
ADD bin/account-app /app

