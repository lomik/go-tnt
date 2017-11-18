# github.com/lomik/go-tnt

Tarantool 1.5 connector.

## Running tests

```
% go test -short ./...
```

To test Tarantool interconnection one must have a test tarantool 1.5 instance running on the primary port 2001.
Start a Docker container defined in `tarantool/Dockerfile`:

```
% make -C tarantool build run
docker build -t tarantool15-test ./
...
docker run -ti --rm -p 2001:2001 tarantool15-test
...
```

Run full test suites:

```
% go test ./...
```