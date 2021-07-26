# Updating goprisma

```
git fetch --all --tags
```

Replace with newest version from:
https://github.com/prisma/prisma-engines/releases

```
git checkout tags/2.27.0 -b master-2.27.0
```

Fix errors if any.
Run integration test:

```
cd query-engine/query-engine-c-api/docker-example
docker-compose up
```

Run tests in `query-engine/query-engine-c-api/src/lib.rs`.

Finally, build from root dir:

Make sure, goprisma exists in `../goprisma`.
Git clone https://github.com/jensneuse/goprisma at same level of this repo.

```
make build-c-api
```

Commit updated binaries to goprisma.
Done! =)