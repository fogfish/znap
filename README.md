# znap

**Znap** is an appliance that enables you to build a snapshot from asynchronous event stream(s), compose an actual state to a particular point in time, and replay it. It is deployable to any cloud environment and out-of-the-box useful. 

By aggregating and merging events using a unique key, Znap's data snapshots mitigate the issue of delays in the recovery service level agreement, often caused by managing large amounts of data.

[![Build Status](https://secure.travis-ci.org/zalando/znap.svg?branch=master)](http://travis-ci.org/zalando/znap)

## Inspiration

Data-streaming/queuing applications such as [Kinesis](https://aws.amazon.com/kinesis/), [SQS](https://aws.amazon.com/sqs/) and Zalando's [Nakadi](https://github.com/zalando/nakadi) typically offer limited retention periods — usually around two weeks. While this is enough time for high-frequency data processing applications to ensure data durability and high-availability, some applications require *permanent* data retention. We also wanted the choice to *replay* a complete data set as it travels through the data stream.

## Getting Started

### ChangeLog
Znap uses [semantic versioning](http://semver.org) to identity stable releases. 

* [0.0.0](https://github.com/zalando/znap/releases/tag/0.0.0) - preview 

### Getting Znap

New versions will be available at Znap's `master` branch.  All development, including new features and bug fixes, take place on the master branch using forking and pull requests, as described in the contribution guidelines. 

### Requirements
To use Znap, you need:
- [Scala](http://www.scala-lang.org)
- [sbt](http://www.scala-sbt.org) 

Znap is built with [STUPS](https://stups.io)'s deployment practices in mind. Future releases will be compatible with AWS tools.   
 
### Running Znap

To stream events, Znap uses REST APIs protected using OAuth2's implicit grant flow (e.g. [Nakadi](https://github.com/zalando/nakadi)). To run it locally, obtain and supply an OAuth2 token to the daemon:

```
export OAUTH2_ACCESS_TOKENS=nakadi=$(zign token)
```

Specify a list of data sources (event streams) where you can fetch and snapshot data: 

```
export ZNAP_STREAMS=https+nakadi://my.nakadi.host.com/my-event-stream1|https+nakadi://my.nakadi.other-host.com/my-event-stream2
```

Run the appliance:

```
sbt run
```

You might also run Znap within a Docker container. Either assemble the container by yourself, or use this community version: `pierone.stups.zalan.do/ie/znap:x.y.z`.

```
docker run -it -e "OAUTH2_ACCESS_TOKENS=${OAUTH2_ACCESS_TOKENS}" -e "ZNAP_STREAMS=${ZNAP_STREAMS}" pierone.stups.zalan.do/ie/znap:x.y.z
```

Assemble the container:

```
make compile VSN=x.y.z
make docker VSN=x.y.z
```

### Deploying Znap
tbd

### Continue to ...
tbd

## Bugs

Please let us know of any problems via [GitHub issues](https://github.com/zalando/znap/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

* **Specify** the configuration of your environment. Include which operating system you use and the versions of your runtime environments. 

* **Attach** logs, screenshots and exceptions, in possible.

* **Reveal** the steps you took to reproduce the problem.

## How to Contribute

We accept contributions via GitHub pull requests: Just fork the repository, then make your changes. Please include a commit message, which helps us to write a good release note and speeds up the review process. The message should address two questions: what changed, and why.

For your commit message, we admire the [Contributing to a Project](http://git-scm.com/book/ch5-2.html) template:
>
> Short (50 chars or less) summary of changes
>
> More detailed explanatory text, if necessary. Wrap it to about 72 characters or so. In some contexts, the first line is treated as the subject of an email and the rest of the text as the body. The blank line separating the summary from the body is critical (unless you omit the body entirely); tools like rebase can get confused if you run the two together.
> 
> Further paragraphs come after blank lines.
> 
> Bullet points are okay, too
> 
> Typically a hyphen or asterisk is used for the bullet, preceded by a single space, with blank lines in between, but conventions vary here
>

## Contacts

* Email: Ivan Yurchenko <ivan.yurchenko@zalando.fi>
* Email: Dmitry Kolesnikov <dmitry.kolesnikov@zalando.fi>


## License

The MIT License (MIT)
Copyright (c) 2016 Zalando SE

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

