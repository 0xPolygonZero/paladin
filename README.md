# Paladin

Bringing divine order to distributed computation.

[Read the docs!](https://0xpolygonzero.github.io/paladin)


## Paladin in 2 sentences

Paladin is a Rust library that aims to simplify writing distributed programs. It provides a declarative API, allowing developers to articulate their distributed programs clearly and concisely, without thinking about the complexities of distributed systems programming.

## Features
- **Declarative API**: Express distributed computations with clarity and ease.
- **Automated Distribution**: Paladin’s runtime seamlessly handles task
  distribution and execution across the cluster.
- **Simplified Development**: Concentrate on the program logic, leaving the complexities of distributed systems to Paladin.
- **Infrastructure Agnostic**: Paladin is generic over its messaging backend and infrastructure provider. Bring your own infra!

## Example
Below is a (contrived) example of how a typical Paladin program looks.

```rust
#[tokio::main]
async fn main() {
    let stream = IndexedStream::from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    // Compute the fibonacci number at each element in the stream.
    let fibs = stream.map(FibAt);
    // Sum the fibonacci numbers.
    let sum = fibs.fold(Sum);

    // Run the computation.
    let result = sum.run(&runtime).await.unwrap();
    assert_eq!(result, 143);
}
```
In this example program, we define an algorithm for computing the fibonacci number at each element in a stream, after which we sum the results. Behind the scenes, Paladin will distribute the computations across the cluster and return the result back to the main thread.

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.


### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.