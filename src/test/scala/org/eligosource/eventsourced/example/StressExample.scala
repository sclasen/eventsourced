/*
 * Copyright 2012 Eligotech BV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eligosource.eventsourced.example

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import org.eligosource.eventsourced.core._

import org.eligosource.eventsourced.example.StressExample._
import org.eligosource.eventsourced.core.Message
import org.eligosource.eventsourced.core.ReliableChannelProps
import concurrent.duration._
import concurrent.{Await, Future}
import com.yammer.metrics.reporting.ConsoleReporter

class StressExample  extends EventsourcingSpec[Fixture] {

  "An event-sourced application" when {
    "using default channels" should {
      "be able to deal with reasonable load" in  { fixture =>
        import fixture._
        val processor = configure(reliable = false)
        println("recovering default")
        extension.recover(2 minutes)
        println("stress default")
        stress(processor, throttle = 100)(Timeout(100 seconds), system)
        queue.poll(100, TimeUnit.SECONDS) must be(cycles)
      }
    }
    "using reliable channels" should {
      "be able to deal with reasonable load" ignore { fixture =>
        import fixture._

        val processor = configure(reliable = true)
        println("recovering reliable")
        extension.recover(2 minutes)
        println("stress reliable")
        stress(processor, throttle = 100)(Timeout(100 seconds), system)
        queue.poll(100, TimeUnit.SECONDS) must be(cycles)
      }
    }
  }
}

object StressExample {
  val cycles = 3000


  class Fixture  extends EventsourcingFixture[Any] {
    val destination = system.actorOf(Props(new Destination(queue) with Receiver with Confirm))

    val reliableChannel = extension.channelOf(ReliableChannelProps(1, destination))
    val defaultChannel = extension.channelOf(DefaultChannelProps(2, destination))

    def configure(reliable: Boolean): ActorRef = {
      val channel = if (reliable) reliableChannel else defaultChannel
      extension.processorOf(Props(new Processor(channel) with Eventsourced { val id = 1 } ))
    }
  }

  def stress(processor: ActorRef, throttle: Long)(implicit timeout: Timeout, system: ActorSystem) {
    import system.dispatcher
    Await.ready(Future.sequence(1 to 100 map {i =>processor ? Message(i)}),20 seconds)
    val start = System.nanoTime()
    1 to cycles foreach { i =>
      //Thread.sleep(throttle)
      val nanos = System.nanoTime()
      processor ? Message(i) onSuccess {
        case r: Int => if (r % 100 == 0) {
          val now = System.nanoTime()

          val latency = (now - nanos) / 1e6
          val throughput = r * 1e9 / (now - start)

          // print some statistics ...
          println("throughput = %.0f msgs/sec, latency of response %d = %.2f ms" format (throughput, r, latency))
        }
      }
    }
  }

  class Processor(channel: ActorRef) extends Actor {
    def receive = {
      case msg: Message => channel forward msg
    }
  }

  class Destination(queue: java.util.Queue[Any]) extends Actor { this: Receiver =>
    var start = System.currentTimeMillis()
    def receive = {
      case ctr: Int => {
        sender ! ctr
        if(ctr % 10 == 0) {
          val time = System.currentTimeMillis() - start
          println(s"cycle:$ctr $time")
        }
        if (ctr == cycles) queue.add(ctr)
      }
    }
  }
}
