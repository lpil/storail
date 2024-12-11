# Stóráil

A simple on-disc JSON based data store.

[![Package Version](https://img.shields.io/hexpm/v/storail)](https://hex.pm/packages/storail)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/storail/)

A super basic on-disc data store that uses JSON files to persist data.

It doesn't have transactions, MVCC, or anything like that. It's just
writing files to disc. Useful for tiny little projects, and for fun.

```sh
gleam add storail@2
```
```gleam
import storail
import my_app

pub fn main() {
  // Construct config to specify where your data is written to.
  let config = storail.Config(
    data_directory: "/data",
    temporary_directory: "/tmp/storail",
  )

  // Define a collection for a data type in your application.
  let cats = storail.Collection(
    name: "cats", 
    to_json: my_app.cat_to_json,
    decoder: my_app.cat_decoder,
    config:,
  )

  // A key points to a specific object within the collection, which 
  // may or may not yet exist.
  let key = storail.key(collection, "nubi")

  // Write some data
  let assert Ok(Nil) = storail.write(key, my_app.Cat("Nubi", 5))

  // Read some data
  storail.read(key)
  // -> Ok(my_app.Cat("Nubi", 5))
}
```

Further documentation can be found at <https://hexdocs.pm/storail>.
