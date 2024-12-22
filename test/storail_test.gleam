import gleam/dict
import gleam/dynamic/decode as de
import gleam/json
import gleam/list
import gleam/option
import gleam/string
import gleeunit
import gleeunit/should
import simplifile
import storail

pub fn main() {
  gleeunit.main()
}

const config = storail.Config(storage_path: "tmp/storage")

fn reset_data() {
  let assert Ok(_) = simplifile.delete_all(["tmp"])
  let assert Ok(_) = simplifile.create_directory("tmp")
  let assert Ok(_) = simplifile.create_directory("tmp/data")
  let assert Ok(_) = simplifile.create_directory("tmp/tmp")
  Nil
}

pub type Cat {
  Cat(name: String, age: Int)
}

fn cat_collection() -> storail.Collection(Cat) {
  let to_json = fn(cat: Cat) {
    json.object([#("name", json.string(cat.name)), #("age", json.int(cat.age))])
  }

  let decoder = {
    use name <- de.field("name", de.string)
    use age <- de.field("age", de.int)
    de.success(Cat(name:, age:))
  }

  storail.Collection(name: "cats", to_json:, decoder:, config:)
}

pub fn read_write_test() {
  reset_data()
  let collection = cat_collection()

  let key = storail.key(collection, "nubi")
  let assert Ok(_) = storail.write(key, Cat("Nubi", 5))
  let assert Ok(Cat("Nubi", 5)) = storail.read(key)
}

pub fn optional_read_test() {
  reset_data()
  let collection = cat_collection()
  let key = storail.key(collection, "nubi")
  let assert Ok(_) = storail.write(key, Cat("Nubi", 5))

  let assert Ok(option.Some(Cat("Nubi", 5))) = storail.optional_read(key)
  let assert Ok(option.None) =
    storail.optional_read(storail.key(collection, "z"))
}

pub fn namespace_test() {
  reset_data()
  let collection = cat_collection()

  let key = storail.namespaced_key(collection, ["louis"], "nubi")
  let assert Error(storail.ObjectNotFound(["louis"], "nubi")) =
    storail.read(key)
  let assert Ok(_) = storail.write(key, Cat("Nubi", 5))
  let assert Ok(Cat("Nubi", 5)) = storail.read(key)

  let key2 = storail.key(collection, "nubi")
  let assert Error(storail.ObjectNotFound([], "nubi")) = storail.read(key2)
}

pub fn overwrite_test() {
  reset_data()
  let collection = cat_collection()

  let key = storail.key(collection, "nubi")
  let assert Ok(_) = storail.write(key, Cat("Nubi", 5))
  let assert Ok(Cat("Nubi", 5)) = storail.read(key)

  let assert Ok(_) = storail.write(key, Cat("Nubi", 6))
  let assert Ok(Cat("Nubi", 6)) = storail.read(key)
}

pub fn not_found_test() {
  reset_data()
  let collection = cat_collection()

  let key = storail.key(collection, "nubi")
  let assert Error(storail.ObjectNotFound([], "nubi")) = storail.read(key)
}

pub fn delete_test() {
  reset_data()
  let collection = cat_collection()

  let key = storail.key(collection, "nubi")
  let assert Ok(_) = storail.write(key, Cat("Nubi", 5))
  let assert Ok(_) = storail.delete(key)
  let assert Error(storail.ObjectNotFound([], "nubi")) = storail.read(key)
}

pub fn read_namespace_test() {
  reset_data()
  let collection = cat_collection()

  let assert Ok(_) =
    storail.namespaced_key(collection, ["0", "1"], "a")
    |> storail.write(Cat("a", 1))
  let assert Ok(_) =
    storail.namespaced_key(collection, ["0", "1"], "b")
    |> storail.write(Cat("b", 2))

  let assert Ok(_) =
    storail.namespaced_key(collection, ["0"], "c")
    |> storail.write(Cat("c", 3))
  let assert Ok(_) =
    storail.namespaced_key(collection, [], "d")
    |> storail.write(Cat("d", 4))

  storail.read_namespace(collection, [])
  |> should.be_ok
  |> should.equal(dict.from_list([#("d", Cat("d", 4))]))

  storail.read_namespace(collection, ["0"])
  |> should.be_ok
  |> should.equal(dict.from_list([#("c", Cat("c", 3))]))

  storail.read_namespace(collection, ["0", "1"])
  |> should.be_ok
  |> should.equal(dict.from_list([#("a", Cat("a", 1)), #("b", Cat("b", 2))]))
}

pub fn list_test() {
  reset_data()
  let collection = cat_collection()

  let assert Ok(_) =
    storail.namespaced_key(collection, ["0", "1"], "a")
    |> storail.write(Cat("a", 1))
  let assert Ok(_) =
    storail.namespaced_key(collection, ["0", "1"], "b")
    |> storail.write(Cat("b", 2))

  let assert Ok(_) =
    storail.namespaced_key(collection, ["0"], "c")
    |> storail.write(Cat("c", 3))
  let assert Ok(_) =
    storail.namespaced_key(collection, [], "d")
    |> storail.write(Cat("d", 4))

  storail.list(collection, [])
  |> should.be_ok
  |> should.equal(["d"])

  storail.list(collection, ["0"])
  |> should.be_ok
  |> should.equal(["c"])

  storail.list(collection, ["0", "1"])
  |> should.be_ok
  |> list.sort(string.compare)
  |> should.equal(["a", "b"])
}
