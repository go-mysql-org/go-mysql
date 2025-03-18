# How to contribute

## Your first contribution

- Best to first create an issue and discuss the problem and possible solution.
  - This isn't needed for small/obvious fixes.
  - What is the context? e.g. what versions of go-mysql, MySQL, etc does this apply to?
  - Do you have a simple test case? This can just be example code, it doesn't have to be a full test case.
  - Why do you need this change?
- Format your code with [gofumpt](https://github.com/mvdan/gofumpt) if possible. (`make fmt` does this for you)
- Reference an issue in the PR if you have one.
- Update the `README.md` if applicable.
- Add tests where applicable.

## Asking for help

- Don't be afraid to ask for help in an issue or PR.

## Testing

Testing is done with GitHub actions.

If you want to run test locally you can use the following flags:

```
  -db string
  -host string
  -pass string
  -port string
  -user string
```

Example:
```
$ cd client
$ go test -args -db test2 -port 3307
```

Testing locally with Docker or Podman can be done like this:
```
podman run \
--rm \
--env MYSQL_ALLOW_EMPTY_PASSWORD=1 \
--env MYSQL_ROOT_HOST='%' \
-p3307:3306 \
-it \
container-registry.oracle.com/mysql/community-server:8.0 \
--gtid-mode=ON \
--enforce-gtid-consistency=ON
```

Substitude `podman` with `docker` if you're using docker. This uses `--rm` to remove the container when it stops. It also enabled GTID by passing options to `mysqld`.
