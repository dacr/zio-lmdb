package zio.lmdb

import zio.lmdb.json.LMDBCodecJson

// Mirror of protobuf class named UserProfilePB - CHANGE IN SYNC
// Used to compare performance between JSON and PROTOBUF
case class UserProfile(
  id: String,
  name: String,
  email: String,
  age: Int
) derives LMDBCodecJson
