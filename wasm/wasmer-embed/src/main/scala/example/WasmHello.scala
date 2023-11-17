package example

import org.wasmer.Instance

import java.nio.file.{Files, Paths};

object Hello extends Greeting with App {
  System.setProperty("os.arch", "arm64")

  val bytes = Files.readAllBytes(Paths.get("../multiply.wasm"))
  val instance = new Instance(bytes)

  val a: Long = 4
  val b: Long = 2
  val resultObj = (instance.exports.getFunction("multiply").apply(a.asInstanceOf[Object], b.asInstanceOf[Object]).asInstanceOf[Array[Any]])(0)

  val result = resultObj match {
    case l: Long => l
    case l: Int => l
    case l: Number => l
    case _ => throw new Exception("Expected a Long")
  }

  instance.close

  println(greeting + " 4 * 2 = " + result)
}

trait Greeting {
  lazy val greeting: String = "hello"  
}
