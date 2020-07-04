# MIT 6.824 Distributed Systems (Spring 2020) course

Notes and code the MIT course. [Official website](https://pdos.csail.mit.edu/6.824/).

## Papers

1. [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

## Development

For labs, I am using Docker image and a shared folder with host machine to write code. Course is using Golang 1.13.

Pull the image:

```bash
docker pull golang:1.13
```

Run the image:

```bash
docker run --rm -v $PWD:/pwd --name go-mit -i golang:1.13
```

## Tips

Use `--race` flag to catch a race condition. It is not a static code analysis, only runtime, so not every part fo the code may be checked.

```bash
go run --race [filename].go
```
