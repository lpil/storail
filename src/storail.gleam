//// A super basic on-disc data store that uses JSON files to persist data.
////
//// It doesn't have transactions, MVCC, or anything like that. It's just
//// writing files to disc.
////
//// Useful for tiny little projects, and for fun.

import decode/zero as de
import filepath
import gleam/dict.{type Dict}
import gleam/json.{type Json}
import gleam/list
import gleam/result
import gleam/string
import simplifile

/// The configuration for your storage. This can be different per-collection if
/// you prefer, but typically you'd use the same for all collections.
///
pub type Config {
  Config(
    /// The directory where the recorded data will be written to.
    /// The contents of this directory should be backed up.
    data_directory: String,
    /// A directory where temporary data will be written to.
    /// This data does not need to be persisted and can be happily lost of
    /// deleted.
    ///
    /// This should be on the same drive as the data directory in order to get
    /// atomic file moving into that directory.
    temporary_directory: String,
  )
}

/// A collection, similar to a table in a relational database.
///
/// Construct this type to be able to read and write values of type to the
/// database.
///
pub type Collection(t) {
  Collection(
    /// The name of the collection. This needs to be suitable for use in file
    /// paths. Typically lowercase plural names are recommended, such as "cats"
    /// and "people".
    name: String,
    /// A function that encodes a value into JSON, ready to be written to the
    /// disc.
    to_json: fn(t) -> Json,
    /// A decoder that transforms the JSON from disc back into the expected
    /// type. 
    ///
    /// If you change the structure of your JSON you will need to make sure this
    /// decoder supports both the new and the old format, otherwise it will fail
    /// when decoding older JSON.
    decoder: de.Decoder(t),
    /// The configuration for this collection. See the `Config` type for
    /// details.
    config: Config,
  )
}

/// A pointer into a collection, where an instance could be written to or read
/// from. Typically this would be constructed with the `key` and
/// `namespaced_key` functions.
///
pub type Key(t) {
  Key(
    /// The collection this key is for.
    collection: Collection(t),
    /// A grouping that this key points into. All objects within a namespace can
    /// be queried at once.
    ///
    /// A use for this may be to create "parents" for object. An "orders"
    /// collection may conceptually belong to a "customer" entity, so you may
    /// choose to give each order a namespace of `["customer", customer_id]`.
    ///
    /// Note that the namespace can be anything, you do not need a "customers"
    /// collection to use `"customers"` in a namespace list.
    namespace: List(String),
    /// The identifier for the object. These are unique per-namespace.
    id: String,
  )
}

pub fn key(collection: Collection(t), id: String) -> Key(t) {
  Key(collection:, namespace: [], id:)
}

pub fn namespaced_key(
  collection: Collection(t),
  namespace: List(String),
  id: String,
) -> Key(t) {
  Key(collection:, namespace:, id:)
}

pub type StorailError {
  /// No object was found for the given key, so there was nothing to read.
  ObjectNotFound(namespace: List(String), id: String)
  /// The object could be read, but it could not be decoded in the desired type.
  CorruptJson(path: String, detail: json.DecodeError)
  /// There was an error working with the filesystem.
  FileSystemError(path: String, detail: simplifile.FileError)
}

fn namespace_path(collection: Collection(t), namespace: List(String)) -> String {
  collection.config.data_directory
  |> filepath.join(collection.name)
  |> list.fold(namespace, _, filepath.join)
}

fn object_data_path(key: Key(t)) -> String {
  namespace_path(key.collection, key.namespace)
  |> filepath.join(key.id <> ".json")
}

// TODO: include a random string to avoid concurrent clobbering
fn object_tmp_path(key: Key(t)) -> String {
  key.collection.config.temporary_directory
  |> filepath.join(key.collection.name <> "-" <> key.id <> ".json")
}

fn ensure_parent_directory_exists(path: String) -> Result(Nil, StorailError) {
  path
  |> filepath.directory_name
  |> simplifile.create_directory_all
  |> result.map_error(FileSystemError(path, _))
}

/// Write an object to the file system.
///
/// Writing is done by writing the JSON to the temporary directory and then by
/// moving to the data directory. Moving on the same file system is an atomic
/// operation for most file systems, so this should avoid data corruption from
/// half-written files when writing was interupted by the VM being killed, the
/// computer being unplugged, etc.
///
/// # Examples
///
/// ```gleam
/// pub fn run(cats: Collection(Cat)) {
///   let cat = Cat(name: "Nubi", age: 5)
///   storail.key(cats, "nubi") |> storail.write(cat)
///   // -> Ok(Nil)
/// }
/// ```
///
pub fn write(key: Key(t), data: t) -> Result(Nil, StorailError) {
  let tmp_path = object_tmp_path(key)
  let data_path = object_data_path(key)

  use _ <- result.try(ensure_parent_directory_exists(tmp_path))
  use _ <- result.try(ensure_parent_directory_exists(data_path))

  // Encode the data to JSON
  let json = data |> key.collection.to_json |> json.to_string

  // Write to the tmp directory first so if writing is interupted then there
  // will be no corrupted half-written files.
  use _ <- result.try(
    simplifile.write(to: tmp_path, contents: json)
    |> result.map_error(FileSystemError(tmp_path, _)),
  )

  // Once written move the file into the data directory. This is an atomic
  // operation on most file systems.
  use _ <- result.try(
    simplifile.rename(at: tmp_path, to: data_path)
    |> result.map_error(FileSystemError(data_path, _)),
  )

  Ok(Nil)
}

fn read_file(
  path path: String,
  namespace namespace: List(String),
  id id: String,
) -> Result(BitArray, StorailError) {
  simplifile.read_bits(path)
  |> result.map_error(fn(error) {
    case error {
      simplifile.Enoent -> ObjectNotFound(namespace, id)
      _ -> FileSystemError(path, error)
    }
  })
}

fn parse_json(
  json: BitArray,
  path: String,
  decoder: de.Decoder(t),
) -> Result(t, StorailError) {
  case json.decode_bits(json, de.run(_, decoder)) {
    Ok(d) -> Ok(d)
    Error(e) -> Error(CorruptJson(path, e))
  }
}

/// Read an object from the file system.
///
/// # Examples
///
/// ```gleam
/// pub fn run(cats: Collection(Cat)) {
///   storail.key(cats, "nubi") |> storail.read
///   // -> Ok(Cat(name: "Nubi", age: 5))
/// }
/// ```
///
pub fn read(key: Key(t)) -> Result(t, StorailError) {
  let path = object_data_path(key)
  use json <- result.try(read_file(path, key.namespace, key.id))
  parse_json(json, path, key.collection.decoder)
}

/// Delete an object from the file system.
///
/// # Examples
///
/// ```gleam
/// pub fn run(cats: Collection(Cat)) {
///   storail.key(cats, "nubi") |> storail.delete
///   // -> Ok(Nil)
/// }
/// ```
///
pub fn delete(key: Key(t)) -> Result(Nil, StorailError) {
  let path = object_data_path(key)
  simplifile.delete_all([path])
  |> result.map_error(FileSystemError(path, _))
}

/// Read all objects from a namespace.
///
/// # Examples
///
/// ```gleam
/// pub fn run(cats: Collection(Cat)) {
///   storail.read_namespace(cats, ["owner", "hayleigh"])
///   // -> Ok([
///   //   Cat(name: "Haskell", age: 3),
///   //   Cat(name: "Agda", age: 2),
///   // ])
/// }
/// ```
///
pub fn read_namespace(
  collection: Collection(t),
  namespace: List(String),
) -> Result(Dict(String, t), StorailError) {
  let path = namespace_path(collection, namespace)
  case simplifile.read_directory(path) {
    Error(e) ->
      case e {
        simplifile.Enoent -> Ok(dict.new())
        _ -> Error(FileSystemError(path, e))
      }
    Ok(contents) ->
      contents
      |> list.filter(string.ends_with(_, ".json"))
      |> list.try_map(fn(filename) {
        let id = filename |> string.drop_right(5)
        let path = filepath.join(path, filename)
        use json <- result.try(read_file(path, namespace, id))
        use data <- result.map(parse_json(json, path, collection.decoder))
        #(id, data)
      })
      |> result.map(dict.from_list)
  }
}
