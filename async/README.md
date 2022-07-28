# Programming Assignment: Programming with Futures (verified track)

You have not submitted. You must earn 8/10 points to pass.

## Deadline

Pass this assignment by Aug 1, 1:59 AM CDT

InstructionsMy submissionsDiscussions

The goal of this assignment is to familiarize yourself with the infrastructure and the tools required during this class.

## Installing Required Tools

Before anything else, _it is crucial that you make sure that all tools are correctly installed_. Take a careful look at the Tools Setup pages and verify that all the listed tools work on your machine.

## The Assignment

### Part 1: Obtain the Project Files

To get started, [download the async.zip](https://moocs.scala-lang.org/~dockermoocs/handouts/scala-2.13/async.zip) handout archive file and extract it somewhere on your machine.

### Part 2: Editing the Project

In case you like using an IDE, import the project into your IDE. If you are new to Scala and don’t know which IDE to choose, we recommend you try IntelliJ. You can find information on how to import a Scala project into IntelliJ in the IntelliJ IDEA Tutorial page.

Then, in the folder _src/main/scala_, open the package _async_ and double-click the file _Async.scala_. This files contains an object whose methods need to be implemented.

When working on an assignment, it is important that you don't change any existing method, class or object names or types. When doing so, our automated grading tools will not be able to recognize your code, and you have a high risk of not obtaining any points for your solution.

### Part 3: Running your Code

Once you start writing some code, you might want to experiment with Scala, execute small snippets of code, or also run some methods that you already implemented. We present two possibilities to run Scala code.

Note that these tools are recommended for exploring Scala, but should not be used for testing your code. The next section of this document will explain how to write tests in Scala.

Using the Scala REPL

In the sbt console, start the Scala REPL by typing _console_:

```bash
> console 
[info] Starting scala interpreter... 
scala>
```

In the REPL you can try out arbitrary snippets of Scala code, for example:

```bash
1 scala> val l = List(3,7,2) 
2  l: List[Int] = List(3, 7, 2)  
3 scala> l.isEmpty 
4  res0: Boolean = false  
5 scala> l.tail.head 
6  res1: Int = 7  
7 scala> List().isEmpty 
8  res2: Boolean = true 
```

The classes of the assignment are available inside the REPL, so you can for instance import all the methods from object Async:

```bash
1 scala> import async.Async._ 
2  import async.Async._  
3 scala> transformSuccess(concurrent.Future.successful(3))
4  res1: scala.concurrent.Future[Boolean] = Future(Success(false)) 
```

In order to exit the Scala REPL and go back to sbt, just type _ctrl-d_.

Using a Main Object

Another way to run your code is to create a new Main object that can be executed by the Java Virtual Machine.

In your code editor, create a new _Main.scala_ file in the _async_ package:

```bash
1 package async
2 object Main extends App {
3   println(Async.transformSuccess(concurrent.Future.successful(3)))
4 }
```

In order to make the object executable it has to extend the type App.

You can run the Main object in the sbt console by simply using the command run.

### Part 5: Testing your Code

Throughout the assignments of this course we will require you to write unit tests for the code that you write. Unit tests are the preferred way to test your code because unlike REPL commands, unit tests are saved and can be re-executed as often as required. This is a great way to make sure that nothing breaks when you have go back later to change some code that you wrote earlier on.

We will be using the ScalaTest testing framework to write our unit tests. Navigate to the folder _src/test/scala_ and open the file _AsyncSuite.scala_ in package async. This file contains a some tests for the methods that need to be implemented.

You can run the tests by invoking the test sbt command. A test report will be displayed, showing you which tests failed and which tests succeeded.

### Part 6: Submitting your Solution

Once you implemented all the required methods and tested you code thoroughly, you can submit it to our graders. Use sbt to submit your code as described in the "SBT tutorial and Submission of Assignments" section of the Tools Setup module.

### How to submit

Copy the token below and run the submission script included in the assignment download. When prompted, use your email address **email withheld**.

Generate new token

Your submission token is unique to you and should not be shared with anyone. You may submit as many times as you like.