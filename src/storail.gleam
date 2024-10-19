//// A super basic on-disc data store that uses JSON files to persist data.
//// For tiny little pet projects, and for fun.

import decode/zero as de
import filepath
import gleam/dict.{type Dict}
import gleam/json.{type Json}
import gleam/list
import gleam/result
import gleam/string
import simplifile

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

pub type Collection(t) {
  Collection(
    name: String,
    to_json: fn(t) -> Json,
    decoder: de.Decoder(t),
    config: Config,
  )
}

pub type Key(t) {
  Key(collection: Collection(t), namespace: List(String), id: String)
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
  ObjectNotFound(namespace: List(String), id: String)
  CorruptJson(path: String, detail: json.DecodeError)
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

pub fn read(key: Key(t)) -> Result(t, StorailError) {
  let path = object_data_path(key)
  use json <- result.try(read_file(path, key.namespace, key.id))
  parse_json(json, path, key.collection.decoder)
}

pub fn delete(key: Key(t)) -> Result(Nil, StorailError) {
  let path = object_data_path(key)
  simplifile.delete_all([path])
  |> result.map_error(FileSystemError(path, _))
}

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
