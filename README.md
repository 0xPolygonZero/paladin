# Paladin

Bringing divine order to distributed computation.

[Read the docs!](https://mir-protocol.github.io/paladin)


## Paladin in 3 sentences
Paladin is a distributed computation library for Rust. Paladin simplifies the challenge of distributing computations over a cluster of machines. With a declarative API, it abstracts the complexity and intricacies of writing distributed systems, ensuring developers can express their distributed programs in a clear and concise manner while achieving maximal parallelism.

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
