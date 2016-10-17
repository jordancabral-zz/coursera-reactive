package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("onemin") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("twomin") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == math.min(a,b)
  }

  property("empty") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }

  property("meldmin") = forAll { (h1: H, h2: H) =>
    val h3 = meld(h1, h2)
    (findMin(h3) == findMin(h1) || findMin(h3) == findMin(h3))
  }

  property("minima") = forAll { h1: H =>
    checkOrderedHeap(h1)
  }

  def checkOrderedHeap(h: H) : Boolean = {
    if (isEmpty(h)) true
    else checkOrderedHeapAcc(findMin(h), deleteMin(h))
  }

  def checkOrderedHeapAcc(acc: A, h: H) : Boolean = {
    if (isEmpty(h)) true
    else {
      val min = findMin(h)
      if (acc < min) checkOrderedHeapAcc(min, deleteMin(h))
      else false
    }

  }

  lazy val genHeap: Gen[H] = for {
    i <- arbitrary[Int]
    j <- insert(i,empty)
    h <- oneOf(empty, genHeap)
  } yield meld(j,h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
