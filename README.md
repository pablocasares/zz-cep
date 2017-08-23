[![CircleCI](https://circleci.com/gh/wizzie-io/normalizer/tree/master.svg?style=shield&circle-token=f543e7c1376dbb7783d7c08abc945acf28e22c46)](https://circleci.com/gh/wizzie-io/normalizer/tree/master)

# Cep

Cep is a complex event processing engine based on Siddhi and Kafka. You only need to define a JSON stream where you specify the process logic and how the message are transformed.
[Try it now!!](https://wizzie.io/cep/getting/getting-started.html)

## Documentation

You can find the docs on the [Docs Website](https://wizzie.io/cep/)

## Getting Started

You can get started with Cep with this [tutorial](https://wizzie.io/cep/getting/base-tutorial.html).

## Compiling Sources

To build this project you can use `maven` tool. 

If you want to build the JAR of the project you can do:

```
mvn clean package
```

If you want to check if it passes all the test:

```
mvn test
```

If you want to build the distribution tar.gz:

```
mvn clean package -P dist
```

If you want to build the docker image, make sure that you have the docker service running:

```
mvn clean package -P docker
```

## Contributing

1. [Fork it](https://github.com/wizzie-io/cep/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request
