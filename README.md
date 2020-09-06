# MIT 6.824 Distributed Systems (Spring 2020) course

My notes and solutions for the 6.824 course. [Official website](https://pdos.csail.mit.edu/6.824/).

## Papers

1. [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
2. GFS
3. VMWare FT
4. Extended Raft

## Development

For labs, I am using Docker image and a shared with host machine folder to write code. Golang **1.13** is used.

Pull the image:

```bash
docker pull golang:1.13
```

Run the image:

```bash
docker run --rm -v $PWD:/pwd --name go-mit -it golang:1.13 bash
```

## Tips

Use `--race` flag to catch a race condition. It is not a static code analysis, only runtime, so not every part fo the code may be checked.

```bash
go run --race [filename].go
```
