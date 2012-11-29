package net.fyrie.redis

import org.specs2._
import akka.testkit.{ filterEvents, EventFilter }
import concurrent.ExecutionContext.Implicits.global
import concurrent.Future

class KeysSpec extends mutable.Specification with TestClient {

  "keys" >> {
    "should fetch keys" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.keys("anshin*").size must_== 2
    }

    "should fetch keys with spaces" ! client { r: RedisClient ⇒
      r.set("anshin 1", "debasish")
      r.set("anshin 2", "maulindu")
      r.sync.keys("anshin*").size must_== 2
    }
  }

  "randomkey" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.randomkey().parse[String].get must startWith("anshin")
    }
  }

  "rename" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.rename("anshin-2", "anshin-2-new")
      r.sync.rename("anshin-2", "anshin-2-new") must throwA[RedisErrorException]("ERR no such key")
    }
  }

  "renamenx" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.renamenx("anshin-2", "anshin-2-new") must_== true
      r.sync.renamenx("anshin-1", "anshin-2-new") must_== false
    }
  }

  "dbsize" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.dbsize() must_== 2
    }
  }

  "exists" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.exists("anshin-2") must_== true
      r.sync.exists("anshin-1") must_== true
      r.sync.exists("anshin-3") must_== false
    }
  }

  "del" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.del(Set("anshin-2", "anshin-1")) must_== 2
      r.sync.del(Set("anshin-2", "anshin-1")) must_== 0
    }
  }

  "type" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.typeof("anshin-2") must_== "string"
    }
  }

  "expire" >> {
    "should give" ! client { r: RedisClient ⇒
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.sync.expire("anshin-2", 1000) must_== true
      r.sync.expire("anshin-3", 1000) must_== false
    }
  }

  /* "quit" >> {
    "should reconnect" ! client { r ⇒
      r.quit
      r.set("key1", "value1")
      r.quit
      r.sync.get("key1").parse[String] must_== Some("value1")
    }
    /*    ignore("should reconnect with many commands") { // ignore until fixed
      (1 to 1000) foreach (_ ⇒ r.incr("incKey"))
      r.quit
      val result1 = r.get("incKey").parse[Int]
      (1 to 1000) foreach (_ ⇒ r.incr("incKey"))
      val result2 = r.get("incKey").parse[Int]
      r.quit
      (1 to 1000) foreach (_ ⇒ r.incr("incKey"))
      val result3 = r.get("incKey").parse[Int]
      result1.get must_==(Some(1000))
      result2.get must_==(Some(2000))
      result3.get must_==(Some(3000))
    }*/
  } */

  "Multi exec commands" >> {
    "should work with single commands" ! client { r: RedisClient ⇒
      r.multi { rq ⇒
        rq.set("testkey1", "testvalue1")
      } map (_ === ())
    }
    "should work with several commands" ! client { r: RedisClient ⇒
      r.multi { rq ⇒
        for {
          _ ← rq.set("testkey1", "testvalue1")
          _ ← rq.set("testkey2", "testvalue2")
          x ← rq.mget(List("testkey1", "testkey2")).parse[String]
        } yield x
      } map (_ === List(Some("testvalue1"), Some("testvalue2")))
    }
    "should work with a list of commands" ! client { r: RedisClient ⇒
      val values = List.range(1, 100)
      Future sequence {
        r.multi { rq ⇒
          val setq = (Queued[Any](()) /: values)((q, v) ⇒ q flatMap (_ ⇒ rq.set(v, v * 2)))
          (setq.map(_ ⇒ List[Future[Option[Int]]]()) /: values)((q, v) ⇒ q flatMap (l ⇒ rq.get(v).parse[Int].map(_ :: l))).map(_.reverse)
        }
      } map (_.flatten === values.map(2*))
    }
    "should throw an error" ! client { r: RedisClient ⇒
      filterEvents(EventFilter[RedisErrorException]("ERR Operation against a key holding the wrong kind of value")) {
        val result = r.multi { rq ⇒
          for {
            _ ← rq.set("a", "abc")
            x ← rq.lpop("a").parse[String]
            y ← rq.get("a").parse[String]
          } yield (x, y)
        }
        for {
          a ← result._1 recover { case _: RedisErrorException ⇒ success }
          b ← result._2
        } yield {
          b === Some("abc")
        }
      }
    }
    "should handle invalid requests" ! client { r: RedisClient ⇒
      filterEvents(EventFilter[RedisErrorException]("ERR Operation against a key holding the wrong kind of value")) {
        val result = r.multi { rq ⇒
          for {
            _ ← rq.set("testkey1", "testvalue1")
            _ ← rq.set("testkey2", "testvalue2")
            x ← rq.mget(List[String]()).parse[String]
            y ← rq.mget(List("testkey1", "testkey2")).parse[String]
          } yield (x, y)
        }
        for {
          a ← result._1 recover { case _: RedisErrorException ⇒ success }
          b ← result._2
        } yield {
          b === List(Some("testvalue1"), Some("testvalue2"))
        }
      }
    }
  }

  "watch" >> {
    "should fail without watch" ! client { r: RedisClient ⇒
      r.set("key", 0) flatMap { _ ⇒
        val clients = List.fill(10)(RedisClient())
        val futures = for (client ← clients; _ ← 1 to 10) yield client.get("key").parse[Int] flatMap { n ⇒ client.set("key", n.get + 1) }
        Future sequence futures flatMap { _ ⇒
          clients foreach (_.disconnect)
          r.get("key").parse[Int] map (_ must_!= Some(100))
        }
      }
    }
    "should succeed with watch" ! client { r: RedisClient ⇒
      r.sync.set("key", 0)
      val futures = for (_ ← 1 to 100) yield {
        r atomic { rw ⇒
          rw watch "key"
          for {
            Some(n) ← rw.get("key").parse[Int]
          } yield rw multi (_.set("key", n + 1))
        }
      }
      Future sequence futures flatMap { _ ⇒
        r.get("key").parse[Int] map (_ === Some(100))
      }
    }
    "should handle complex request" ! client { r: RedisClient ⇒
      r.sync.rpush("mykey1", 5)
      r.set("mykey2", "hello")
      r.hset("mykey3", "hello", 7)
      val result = r atomic { rw ⇒
        for {
          _ ← rw.watch("mykey1")
          _ ← rw.watch("mykey2")
          _ ← rw.watch("mykey3")
          Some(a) ← rw.lindex("mykey1", 0).parse[Int]
          Some(b) ← rw.get("mykey2").parse[String]
          Some(c) ← rw.hget("mykey3", b).parse[Int]
        } yield rw.multi { rq ⇒
          for {
            _ ← rq.rpush("mykey1", a + 1)
            _ ← rq.hset("mykey3", b, c + 1)
          } yield (a, b, c)
        }
      }
      for {
        a ← result
        b ← r.lrange("mykey1").parse[Int]
        c ← r.hget("mykey3", "hello").parse[Int]
      } yield {
        a === (5, "hello", 7)
        b === Some(List(5, 6))
        c === Some(8)
      }
    }
  }

  "sort" >> {
    "should do a simple sort" ! client { r: RedisClient ⇒
      List(6, 3, 5, 47, 1, 1, 4, 9) foreach (r.lpush("sortlist", _))
      r.sync.sort("sortlist").parse[Int].flatten must_== List(1, 1, 3, 4, 5, 6, 9, 47)
    }
    "should do a lexical sort" ! client { r: RedisClient ⇒
      List("lorem", "ipsum", "dolor", "sit", "amet") foreach (r.lpush("sortlist", _))
      List(3, 7) foreach (r.lpush("sortlist", _))
      r.sync.sort("sortlist", alpha = true).parse[String].flatten must_== List("3", "7", "amet", "dolor", "ipsum", "lorem", "sit")
    }
    "should return an empty list if key not found" ! client { r: RedisClient ⇒
      r.sync.sort("sortnotfound") must beEmpty
    }
    "should return multiple items" ! client { r: RedisClient ⇒
      val list = List(("item1", "data1", 1, 4),
        ("item2", "data2", 2, 8),
        ("item3", "data3", 3, 1),
        ("item4", "data4", 4, 6),
        ("item5", "data5", 5, 3))
      for ((key, data, num, rank) ← list) {
        r.quiet.sadd("items", key)
        r.quiet.set("data::" + key, data)
        r.quiet.set("num::" + key, num)
        r.quiet.set("rank::" + key, rank)
      }
      r.quiet.del(List("num::item1"))
      r.sync.sort("items",
        get = Seq("#", "data::*", "num::*"),
        by = Some("rank::*"),
        limit = Limit(1, 3)).parse[String] must_== List(Some("item5"), Some("data5"), Some("5"),
          Some("item1"), Some("data1"), None,
          Some("item4"), Some("data4"), Some("4"))
      r.sync.sort("items",
        get = Seq("#", "data::*", "num::*"),
        by = Some("rank::*"),
        limit = Limit(1, 3)).parse[String, String, Int] must_== (List((Some("item5"), Some("data5"), Some(5)),
          (Some("item1"), Some("data1"), None),
          (Some("item4"), Some("data4"), Some(4))))
    }
  }

}

