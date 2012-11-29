package net.fyrie.redis
package commands

import serialization._
import akka.util.ByteString
import concurrent.ExecutionContext

private[redis] trait Hashes[Result[_]] {
  this: Commands[Result] ⇒
  import protocol.Constants._

  def hdel[K: Store, F: Store](key: K, field: F)(implicit executor: ExecutionContext): Result[Boolean] =
    send(HDEL :: Store(key) :: Store(field) :: Nil)

  def hexists[K: Store, F: Store](key: K, field: F)(implicit executor: ExecutionContext): Result[Boolean] =
    send(HEXISTS :: Store(key) :: Store(field) :: Nil)

  def hget[K: Store, F: Store](key: K, field: F)(implicit executor: ExecutionContext): Result[Option[ByteString]] =
    send(HGET :: Store(key) :: Store(field) :: Nil)

  def hgetall[K: Store](key: K)(implicit executor: ExecutionContext): Result[Map[ByteString, ByteString]] =
    send(HGETALL :: Store(key) :: Nil)

  def hincrby[K: Store, F: Store](key: K, field: F, value: Long = 1)(implicit executor: ExecutionContext): Result[Long] =
    send(HINCRBY :: Store(key) :: Store(field) :: Store(value) :: Nil)

  def hkeys[K: Store](key: K)(implicit executor: ExecutionContext): Result[Set[ByteString]] =
    send(HKEYS :: Store(key) :: Nil)

  def hlen[K: Store](key: K)(implicit executor: ExecutionContext): Result[Long] =
    send(HLEN :: Store(key) :: Nil)

  def hmget[K: Store, F: Store](key: K, fields: Seq[F])(implicit executor: ExecutionContext): Result[List[Option[ByteString]]] =
    send(HMGET :: Store(key) :: (fields.map(Store(_))(collection.breakOut): List[ByteString]))

  def hmset[K: Store, F: Store, V: Store](key: K, fvs: Iterable[Product2[F, V]])(implicit executor: ExecutionContext): Result[Unit] =
    send(HMSET :: Store(key) :: (fvs.flatMap(fv ⇒ Iterable(Store(fv._1), Store(fv._2)))(collection.breakOut): List[ByteString]))

  def hset[K: Store, F: Store, V: Store](key: K, field: F, value: V)(implicit executor: ExecutionContext): Result[Boolean] =
    send(HSET :: Store(key) :: Store(field) :: Store(value) :: Nil)

  def hsetnx[K: Store, F: Store, V: Store](key: K, field: F, value: V)(implicit executor: ExecutionContext): Result[Boolean] =
    send(HSETNX :: Store(key) :: Store(field) :: Store(value) :: Nil)

  def hvals[K: Store](key: K)(implicit executor: ExecutionContext): Result[Set[ByteString]] =
    send(HVALS :: Store(key) :: Nil)

}

