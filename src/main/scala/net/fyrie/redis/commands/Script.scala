package net.fyrie.redis
package commands

import serialization._
import akka.util.ByteString
import concurrent.ExecutionContext

private[redis] trait Scripts[Result[_]] {
  this: Commands[Result] ⇒
  import protocol.Constants._

  def eval[S: Store, K: Store, A: Store](script: S, keys: Seq[K] = Seq.empty[Store.Dummy], args: Seq[A] = Seq.empty[Store.Dummy])(implicit executor: ExecutionContext): Result[types.RedisType] =
    send(EVAL :: Store(script) :: Store(keys.size) :: (keys.map(Store(_))(collection.breakOut): List[ByteString]) ::: (args.map(Store(_))(collection.breakOut): List[ByteString]))

}
