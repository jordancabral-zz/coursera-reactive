package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class RetryPersistence(key: String, valueOption: Option[String], id: Long)
  case class PersistenceAndReplicationTimeout(key: String, valueOption: Option[String], id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // percistence Actor
  val persistence = context.actorOf(persistenceProps)

  override val supervisorStrategy = OneForOneStrategy(){
    case _ => Restart
  }

  var sentToPersist = Set.empty[Long]
  var sentToreplicate = Map.empty[Long, Set[ActorRef]]
  var upsertSenders = Map.empty[Long, ActorRef]

  arbiter ! Join

  var _seqCounter = 0L
  def nextSeq = {
    _seqCounter += 1
  }

  var _replicationCounter = 0L
  def nextReplicationId = {
    val ret = _replicationCounter
    _replicationCounter += 1
    ret
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def isReplicatedAndPersisted(id: Long) = {
    sentToreplicate.getOrElse(id, Set.empty).isEmpty  && !sentToPersist.contains(id)
  }

  def replicateAndPersist(key: String, value: Option[String], id: Long): Unit ={
    replicators.foreach(_ ! Replicate(key, value, id))
    persistence ! Persist(key, value, id)
    sentToPersist = sentToPersist + id
    sentToreplicate = sentToreplicate + ((id, replicators))
    upsertSenders = upsertSenders + ((id, sender()))
    context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersistence(key, value, id))
    context.system.scheduler.scheduleOnce(1 seconds, self, PersistenceAndReplicationTimeout(key, value, id))
  }

  /* Behavior for  the leader role. */
  val leader: Receive = {

    case Insert(k, v, id) => {
      kv = kv + ((k,v))
      replicateAndPersist(k, Some(v), id)
    }

    case Remove(k, id) => {
      kv = kv - k
      replicateAndPersist(k, None, id)
    }

    case Get(k, id) => {
      sender() ! GetResult(k, kv.get(k), id)
    }

    case Replicas(replicas) => {
      val newReplicas = (replicas -- secondaries.keySet) - context.self
      val deletedReplicas = secondaries.keySet -- replicas

      deletedReplicas.foreach(context.stop(_))
      (secondaries filterKeys deletedReplicas).values.foreach(context.stop(_))

      sentToreplicate = sentToreplicate.mapValues(_.filter(deletedReplicas.contains(_)))

      val newPairs = for (replica <- newReplicas) yield (replica, context.actorOf(Replicator.props(replica)))

      secondaries = (secondaries filterKeys deletedReplicas) ++ newPairs
      replicators = secondaries.values.toSet

      for {
        (_, replicator) <- newPairs
        (key, value) <- kv
      } yield replicator ! Replicate(key, Option(value), nextReplicationId)
    }

    case Replicated(key, id) => {
      if (upsertSenders.contains(id)) {
        sentToreplicate = sentToreplicate + ((id, sentToreplicate(id) - (context.sender())))
        if (isReplicatedAndPersisted(id)) {
          val requester = upsertSenders(id)
          requester ! OperationAck(id)
          upsertSenders = upsertSenders - id
          sentToreplicate = sentToreplicate - id
        }
      }
    }

    case Persisted(key, id) => {
      if (upsertSenders.contains(id)) {
        sentToPersist = sentToPersist - id
        if (isReplicatedAndPersisted(id)) {
          val requester = upsertSenders(id)
          requester ! OperationAck(id)
          upsertSenders = upsertSenders - id
        }
      }
    }

    case RetryPersistence(key, valueOption, id) => {
      if (sentToPersist.contains(id)) {
        persistence ! Persist(key, valueOption, id)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersistence(key, valueOption, id))
      }
    }

    case PersistenceAndReplicationTimeout(key, valueOption, id) => {
      if (upsertSenders.contains(id)) {
        val requester = upsertSenders(id)
        if (!isReplicatedAndPersisted(id)) {
          requester ! OperationFailed(id)
        } else {
          requester ! OperationAck(id)
        }
        upsertSenders = upsertSenders - id
      }
    }



  }

  /* Behavior for the replica role. */
  val replica: Receive = {

    case Get(k, id) => {
      sender() ! GetResult(k, kv.get(k), id)
    }

    case Snapshot(key, valueOption, seq) => {
      if (seq > _seqCounter) ()
      else if (seq < _seqCounter) {sender() ! SnapshotAck(key, seq)}
      else {
        valueOption match {
          case None        => {
            kv = kv - key
            persistence ! Persist(key, None, seq)
          }
          case Some(value) => {
            kv = kv + ((key, value))
            persistence ! Persist(key, Option(value), seq)
          }
        }
        nextSeq
        sentToPersist = sentToPersist + seq
        upsertSenders = upsertSenders + ((seq, sender()))
        context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersistence(key, valueOption, seq))
      }
    }

    case Persisted(key, id) => {
      val requester = upsertSenders(id)
      requester ! SnapshotAck(key, id)
      sentToPersist = sentToPersist - id
    }

    case RetryPersistence(key, valueOption, id) => {
      if (sentToPersist.contains(id)) {
        persistence ! Persist(key, valueOption, id)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersistence(key, valueOption, id))
      }
    }

  }

}

