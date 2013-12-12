package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import akka.event.LoggingReceive
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

  // the last acknowledged sequence number
  var lastAckSeq = -1L

  // the expected sequence number
  var expectedSeq = 0L

  var persister = context.actorOf(persistenceProps)
  context.watch(persister)

  // the outstanding persistence request sent by the secondary
  var acks = Map.empty[Long, (ActorRef, Snapshot)]

  // the outstanding persistence request sent by the primary
  var persists = Map.empty[Long, (ActorRef, Replicate)]

  // sequence number used for persistence request
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
     case _: PersistenceException => SupervisorStrategy.Restart
  }

  context.system.scheduler.schedule(0.milliseconds, 100.milliseconds) {
    acks foreach { case (id, (_, Snapshot(key, valueOption, seq))) => {
        persister ! Persist(key, valueOption, id)
      }
    }
    persists foreach { case (id, (_, Replicate(key, valueOption, seq))) => {
        persister ! Persist(key, valueOption, id)
      }
    }
  }

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) => {
      kv += key -> value
      val seq = nextSeq
      val replicate = Replicate(key, Some(value), id)
      replicators foreach { r => r ! replicate }
      persists += seq -> (sender, replicate)
      persister ! Persist(key, Some(value), seq)

      // schedule for timeout for persistence request
      context.system.scheduler.scheduleOnce(1.second) {
        if (persists contains seq) {
          val persist = persists.get(seq)
          persist match {
            case Some((client, Replicate(k, v, id))) => {
              persists -= seq
              client ! OperationFailed(id)
            }
            case None => 
          }
        }
      }
    }
    case Remove(key, id) => {
      kv -= key
      val seq = nextSeq
      val replicate = Replicate(key, None, id)
      replicators foreach { r => r ! Replicate(key, None, id) }
      persists += seq -> (sender, replicate)
      persister ! Persist(key, None, seq)

      // schedule for timeout for persistence request
      context.system.scheduler.scheduleOnce(1.second) {
        if (persists contains seq) {
          val persist = persists.get(seq)
          persist match {
            case Some((client, Replicate(k, v, id))) => {
              persists -= seq
              client ! OperationFailed(id)
            }
            case None => 
          }
        }
      }
    }
    case Get(key, id) => {
      val opt = kv.get(key)
      sender ! GetResult(key, opt, id)
    }
    case Replicas(replicas) => {
      val others = replicas - self
      val added = others filter { !secondaries.contains(_) }
      added foreach { s =>
        val replicator = context.actorOf(Replicator.props(s))
        secondaries += s -> replicator
        replicators += replicator
      }
    }
    case Persisted(key, seq) => {
      if (persists contains seq) {
        val persist = persists.get(seq)
        persist match {
          case Some((client, Replicate(k, v, id))) => {
            persists -= seq
            client ! OperationAck(id)
          }
          case None => 
        }
      }
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) => {
      val opt = kv.get(key)
      sender ! GetResult(key, opt, id)
    }
    case Snapshot(key, valueOption, seq) => {
      val expected = Math.max(expectedSeq, lastAckSeq + 1)
      if (seq < expected) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == expected) {
        valueOption match {
          case Some(value) => { kv += key -> value }
          case None => { kv -= key }
        }
        val id = nextSeq
        acks += id -> (sender, Snapshot(key, valueOption, seq))
        persister ! Persist(key, valueOption, id)
      }
    }
    case Persisted(key, id) => {
      if (acks contains id) {
        val ack = acks.get(id)
        ack match {
          case Some((replicator, Snapshot(k, v, seq))) => {
            acks -= id
            lastAckSeq = seq
            expectedSeq = lastAckSeq + 1
            replicator ! SnapshotAck(key, seq)
          }
          case None => 
        }
      }
    }
  }

}
