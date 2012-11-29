package net.fyrie.redis
package commands

import serialization._
import akka.util.ByteString
import concurrent.ExecutionContext

private[redis] trait Strings[Result[_]] {
  this: Commands[Result] ⇒
  import protocol.Constants._

  def append[K: Store, V: Store](key: K, value: V)(implicit executor: ExecutionContext): Result[Int] =
    send(APPEND :: Store(key) :: Store(value) :: Nil)

  def decr[K: Store](key: K)(implicit executor: ExecutionContext): Result[Long] =
    send(DECR :: Store(key) :: Nil)

  def decrby[K: Store](key: K, decrement: Long)(implicit executor: ExecutionContext): Result[Long] =
    send(DECRBY :: Store(key) :: Store(decrement) :: Nil)

  def get[K: Store](key: K)(implicit executor: ExecutionContext): Result[Option[ByteString]] =
    send(GET :: Store(key) :: Nil)

  def getbit[K: Store](key: K, offset: Int)(implicit executor: ExecutionContext): Result[Int] =
    send(GETBIT :: Store(key) :: Store(offset) :: Nil)

  def getrange[K: Store](key: K, start: Int = 0, end: Int = -1)(implicit executor: ExecutionContext): Result[Int] =
    send(GETRANGE :: Store(key) :: Store(start) :: Store(end) :: Nil)

  def getset[K: Store, V: Store](key: K, value: V)(implicit executor: ExecutionContext): Result[Option[ByteString]] =
    send(GETSET :: Store(key) :: Store(value) :: Nil)

  def incr[K: Store](key: K)(implicit executor: ExecutionContext): Result[Long] =
    send(INCR :: Store(key) :: Nil)

  def incrby[K: Store](key: K, increment: Long)(implicit executor: ExecutionContext): Result[Long] =
    send(INCRBY :: Store(key) :: Store(increment) :: Nil)

  def mget[K: Store](keys: Seq[K])(implicit executor: ExecutionContext): Result[List[Option[ByteString]]] =
    send(MGET :: (keys.map(Store(_))(collection.breakOut): List[ByteString]))

  def mset[K: Store, V: Store](kvs: Iterable[Product2[K, V]])(implicit executor: ExecutionContext): Result[Unit] =
    send(MSET :: (kvs.flatMap(kv ⇒ Iterable(Store(kv._1), Store(kv._2)))(collection.breakOut): List[ByteString]))

  def msetnx[K: Store, V: Store](kvs: Iterable[Product2[K, V]])(implicit executor: ExecutionContext): Result[Boolean] =
    send(MSETNX :: (kvs.flatMap(kv ⇒ Iterable(Store(kv._1), Store(kv._2)))(collection.breakOut): List[ByteString]))

  def set[K: Store, V: Store](key: K, value: V)(implicit executor: ExecutionContext): Result[Unit] =
    send(SET :: Store(key) :: Store(value) :: Nil)

  def setbit[K: Store](key: K, offset: Int, value: Int)(implicit executor: ExecutionContext): Result[Int] =
    send(SETBIT :: Store(key) :: Store(offset) :: Store(value) :: Nil)

  def setex[K: Store, V: Store](key: K, seconds: Int, value: V)(implicit executor: ExecutionContext): Result[Unit] =
    send(SETEX :: Store(key) :: Store(seconds) :: Store(value) :: Nil)

  def setnx[K: Store, V: Store](key: K, value: V)(implicit executor: ExecutionContext): Result[Boolean] =
    send(SETNX :: Store(key) :: Store(value) :: Nil)

  def setrange[K: Store, V: Store](key: K, offset: Int, value: V)(implicit executor: ExecutionContext): Result[Int] =
    send(SETRANGE :: Store(key) :: Store(offset) :: Store(value) :: Nil)

  def strlen[K: Store](key: K)(implicit executor: ExecutionContext): Result[Int] =
    send(STRLEN :: Store(key) :: Nil)
}

