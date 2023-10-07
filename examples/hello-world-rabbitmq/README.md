# Hello World RabbitMQ example

This is a simple example showcasing distributed generation of the string
"hello world!" using Paladin and RabbitMQ. 

A `docker-compose.yml` is provided to run the example. Calling `docker compose up` will start a RabbitMQ instance, 6 Paladin workers, and a single leader. If you haven't built the containers yet, you can do so with `docker compose up --build`.

The leader in this example contains the following code:

```rust
let input = ['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!'];
let computation = IndexedStream::from(input)
    .map(CharToString)
    .fold(StringConcat);

let result = computation.run(&runtime).await?;
assert_eq!(result, "hello world!".to_string());
```

We're exercising both `map` and `fold` to go from a sequence of characters to a fully concatenated string. The `assert_eq` verifies that the result is as expected.

After running, you can view leader's output with `docker compose logs leader`.
