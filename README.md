# znap

`znap` is the appliance as-is deployable to cloud environment. It builds a snapshot from asynchronous event stream(s), compose an actual state to point in time and provides an ability to replay it.    

[![Build Status](https://secure.travis-ci.org/zalando/znap.svg?branch=master)](http://travis-ci.org/zalando/znap)

## Inspiration

There is a limited retention period at data streaming/queuing application such as [Kinesis](https://aws.amazon.com/kinesis/), [SQS](https://aws.amazon.com/sqs/) or [Nakadi](https://github.com/zalando/nakadi). These applications provides long-enough retention period (e.g. up to 2 weeks) for high-frequency data processing application to ensure data durability and high-availability. However, some application requires *permanent* data retention. There should be ability to *replay* complete data set traversed through the stream.

The log replay recovery works for relatively short logs otherwise amount of data delays recovery service level agreement. The data snapshots mitigates this issue by aggregating and merging events using unique key.


## Getting Started

### ChangeLog
The project uses [semantic versions](http://semver.org) to identity stable releases. 

* [0.0.0](https://github.com/zalando/znap/releases/tag/0.0.0) - preview 

### Requirements
To develop `znap`, you need:
- [Scala](http://www.scala-lang.org)
- [sbt](http://www.scala-sbt.org) 
- [stups](https://stups.io) Note, the appliance is biased to stups deployment practices. Further releases will be compatible with vanilla AWS tools.   

### Getting znap

The latest version of `znap` is available at its `master` branch.  All development, including new features and bug fixes, take place on the master branch using forking and pull requests as described in contribution guidelines. 


### Running znap

The appliance uses REST APIs to stream events (e.g. [Nakadi API](https://github.com/zalando/nakadi)). These APIs are protected using OAuth2 implicit grant flow. You need to obtains and supply OAuth2 token to daemon in order to run it locally.   

```
export OAUTH2_ACCESS_TOKENS=nakadi=$(zign token)
```

You need to specify list of data sources (event streams) where to fetch and snapshot data. 

```
export ZNAP_STREAMS=https+nakadi://my.nakadi.host.com/my-event-stream1|https+nakadi://my.nakadi.other-host.com/my-event-stream2
```

Run the appliance 

```
sbt run
```

You might also run the appliance within the docker container, either assemble container by your self or use community version `pierone.stups.zalan.do/ie/znap:x.y.z`

```
docker run -it -e "OAUTH2_ACCESS_TOKENS=${OAUTH2_ACCESS_TOKENS}" -e "ZNAP_STREAMS=${ZNAP_STREAMS}" pierone.stups.zalan.do/ie/znap:x.y.z
```

Assemble container 

```
make compile VSN=x.y.z
make docker VSN=x.y.z
```

### Deploying znap
tbd

### Continue to ...
tbd


## How to contribute

`znap` is MIT licensed and accepts contributions via GitHub pull requests:

* Fork the repository on GitHub
* Read the README.md for getting started instructions

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

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




## Bugs

If you experience any issues with `znap`, please let us know via [GitHub issues](https://github.com/zalando/znap/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

* **Specify** the configuration of your environment. Include which operating system you use and the versions of runtime environments. 

* **Attach** logs, screenshots and exceptions, in possible.

* **Reveal** the steps you took to reproduce the problem.



## Contacts

* Email: Ivan Yurchenko <ivan.yurchenko@zalando.fi>
* Email: Dmitry Kolesnikov <dmitry.kolesnikov@zalando.fi>


## License

The MIT License (MIT)
Copyright (c) 2016 Zalando SE

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

