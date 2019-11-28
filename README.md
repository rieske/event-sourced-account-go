## Event Sourced Account

[![Actions Status](https://github.com/rieske/event-sourced-account-go/workflows/build/badge.svg)](https://github.com/rieske/event-sourced-account-go/actions)

Event sourced Account implementation in go.

### Implementation

Replicates the functionality of the [java version](https://github.com/rieske/event-sourced-account).
Those are my first steps at learning go language so the code will not shine.

### API

- open account: `POST /api/account/{accountId}?owner={ownerId}` should respond with `201`
  and a `Location` header pointing to the created resource if successful
- get account's current state: `GET /api/account/{accountId}` should respond with `200`
  and a json body if account is found, otherwise `404`
- deposit: `PUT /api/account/{accountId}?deposit={amount}&transactionId={uuid}`
  should respond with `204` if successful
- withdraw: `PUT /api/account/{accountId}?withdraw={amount}&transactionId={uuid}`
  should respond with `204` if successful
- transfer: `PUT /api/account/{accountId}?transfer={targetAccountId}&amount={amount}&transactionId={uuid}`
  should respond with `204` if successful
- close account: `DELETE /api/account/{accountId}` should respond with `204` if successful


### Tests

Tests without any tag are the fast unit tests and are the ones that run during the build phase.
Normally, I would hook the remaining tests to the build, however since integration and end to end
tests depend on docker daemon, I wanted to avoid broken builds on machines that might not have it set up.

Next level are the integration tests that use MySql backed event store. Tagged with `integration`.

Finally, a couple of end to end tests that focus mainly on sanity testing consistency in a distributed
environment. Tagged with `e2e`.

### Building

```
make
```

The build will only run the fast unit tests.

In order to run the functional integration tests targeting containerized MySql, run
```
make integration-test
```
Those will be much slower - they spawn the actual mysql instance using testcontainers-go and thus
require a running docker daemon on the host.

And another round of slow tests that test for consistency in a distributed environment:
```
make e2e-test
```
Those will spawn a docker-composed environment with two service instances connected to
a mysql container and a load balancer on top. Tests will be executed against the load balancer,
simulating a distributed environment and asserting that the service can scale and remain consistent.


In order to run the build with all test levels: unit, integration and end to end, run
```
make full
```

### Running

Service can be started with an in memory event store implementation using
```
make run
```
Service will start on localhost:8080

The same, but packaged in a docker container:
```
make docker-run
```

Alternatively, two instances packaged in a docker container, connected to a mysql container and
exposed via Envoy Proxy load balancer using:
```
make compose-run
```
This time the service will also be accessible on localhost:8080, just that this time requests
will go via a load balancer to two service instances in a round robin fashion and with a shared
mysql database.

### Monitoring

Basic metrics are exposed to Prometheus and sample configuration of Prometheus together with 
Grafana and a service/envoy dashboards can be accessed by spawning a composed environment using
```
make compose-run
```
Prometheus is exposed on port 9090 and Grafana is available on port 3000.
