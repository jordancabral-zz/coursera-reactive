/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(r, i, e) => insert(r, i, e)
    case Remove(r, i, e) => remove(r, i, e)
    case Contains(r, i, e) => contains(r, i, e)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???

  def insert(requester: ActorRef, id: Int, element: Int): Unit = {
    if (element < elem) insertInSubTreesOrCreateActor(requester, Left, id, element)
    else if (element > elem) insertInSubTreesOrCreateActor(requester, Right, id, element)
    else {
      removed = false
      requester ! OperationFinished(id)
    }
  }

  def insertInSubTreesOrCreateActor(requester: ActorRef, position: Position, id: Int, element: Int): Unit = {
    if (subtrees contains position) subtrees(position) ! Insert(requester, id, element)
    else {
      subtrees = subtrees + ((position, context.actorOf(BinaryTreeNode.props(element, false))))
      requester ! OperationFinished(id)
    }
  }

  def remove(requester: ActorRef, id: Int, element: Int): Unit ={
    if (element < elem) removeInSubTrees(requester, Left, id, element)
    else if (element > elem) removeInSubTrees(requester, Right, id, element)
    else {
      removed = true
      requester ! OperationFinished(id)
    }
  }

  def removeInSubTrees(requester: ActorRef, position: Position, id: Int, element: Int): Unit ={
    if (subtrees contains position) subtrees(position) ! Remove(requester, id, element)
    else  requester ! OperationFinished(id)
  }

  def contains(requester: ActorRef, id: Int, element: Int): Unit ={
    if (element < elem) containsInSubTrees(requester, Left, id, element)
    else if (element > elem) containsInSubTrees(requester, Right, id, element)
    else if (removed)  requester ! ContainsResult(id, false)
    else requester ! ContainsResult(id, true)
  }

  def containsInSubTrees(requester: ActorRef, position: Position, id: Int, element: Int): Unit ={
    if (subtrees contains position) subtrees(position) ! Contains(requester, id, element)
    else requester ! ContainsResult(id, false)
  }

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(r, id, e) => root ! Insert(r, id, e)
    case Remove(r, id, e) => root ! Remove(r, id, e)
    case Contains(r, id, e) => root ! Contains(r, id, e)
    //case GC => context.become(garbageCollecting)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case Insert(r, id, e) => pendingQueue :+ Insert(r, id, e)
    case Remove(r, id, e) => pendingQueue :+ Remove(r, id, e)
    case Contains(r, id, e) => pendingQueue :+ Contains(r, id, e)
  }
}
