use rand::{distributions::Alphanumeric, Rng};

pub fn get_random_routing_key() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}
