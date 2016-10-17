package kvstore

import akka.actor.{ReceiveTimeout, Props, Actor, ActorRef}
import kvstore.Replicator
import kvstore.Replicator.Snapshot
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class RetrySnapshot(seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case Replicate(key, valueOption, id) => {
      val actualseq = nextSeq
      acks = acks + ((actualseq, (sender, Replicate(key, valueOption, id))))
      replica ! Snapshot(key, valueOption, actualseq)
      context.system.scheduler.scheduleOnce(100 milliseconds, self, RetrySnapshot(actualseq))
    }

    case SnapshotAck(key, seq) => {
      if (acks.contains(seq)) {
        val (sender, replica) = acks(seq)
        sender ! Replicated(key, replica id)
        acks = acks - seq
      }
    }

    case RetrySnapshot(seq) => {
      if (acks.contains(seq)) {
        val(_, rep) = acks(seq)
        replica ! Snapshot(rep.key, rep.valueOption, seq)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, RetrySnapshot(seq))
      }
    }

  }

}
