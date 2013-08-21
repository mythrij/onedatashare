Introduction
============

Stork is a data transfer scheduler that provides a common interface
to different file transfer protocols.

Stork uses a client-server architecture where clients submit jobs to
a Stork server and the Stork server performs the transfer when
resources permit. The transfer happens asynchronously to the client,
allowing users to go along their merry way and check on the status
of the job at their own leisure. The Stork server responds to any
failures that may occur during transfer automatically, handling them
in an appropriate way and informing the user if a job can't be
completed.

Stork plug-ins can be created to add support for new file transfer
protocols very easily (with any programming language, too) using a
simple external executable interface. If additional performance or
integration with the Stork server is desired, plug-ins can also be
written in Java and extend the built-in TransferModule class,
eliminating the communication overhead of piping serialized messages
between Stork and the transfer module.

Supported Platforms
===================

Stork is intended to run in any Java Virtual Machine on any modern
operating system.

In reality it's only been tested with the Oracle Java SE 6 and 7 JRE
on Linux, though there's no reason to believe it couldn't work on any
other JVM.

Building
========

On most systems, simply run `make'. For systems without a make
command, the entire source tree can be found

Installation
============

Right now there's no automatic installation. There will be, just not
right now. If you want to install Stork system-wide, after building
you can copy this entire directory into a 

Prerequisites
=============

The following libraries are required to build and run Stork:

* Apache Commons Logging 1.1
* Log4J 1.2.15
* JGlobus 1.8.0
* Netty 4.0.0
* JSch 0.1.50

I'm not sure how far back or forward you can go version-wise, those
are just the versions I've got in my lib directory. I'll formalize
this section a bit later.

Commands
========

stork server
------------
Used to start (or stop) a Stork server.

stork q
-------
List all the jobs in the Stork queue along with information
about them, such as their status and progress. Can be used to find
information about specific jobs by passing a job id. Can also be
used to filter jobs by their status.

stork submit
------------
Submit a job to a Stork server. Can be passed a source
and destination URL, or a LiteAd descriptor of the job.

stork rm
--------
Remove or terminate a submitted job or set of jobs.

stork info
----------
Display configuration information about the server. Can
also be used to find information about transfer modules.

stork ls
--------
List a remote URL.

More information can be found by running `stork --help`.

Configuring
===========

The Stork configuration file (stork.conf) can be used to change
settings for the server and client tools. The search order for the
configuration file is as follows:

 1) $STORK_CONFIG
 2) ~/.stork.conf
 3) /etc/stork.conf
 4) $STORK/stork.conf
 5) /usr/local/stork/stork.conf
 6) stork.conf in currect directory

Even if the file can't be found automatically, every valid config
variable has a "sane" default value. The Stork server will issue a
warning on startup if a config file cannot be found.

How to Use
==========

Start a Stork server, unless you plan on using an existing server.
Submit a job to the server using `stork submit`. Upon submission, the
job will be assigned a job id which stork_submit will output. Run
`stork q all` to view all jobs and look for the job you submitted.
You can use `stork rm` to cancel the job. You can run `stork_info' to
see additional information about a server, such as what protocols it
supports.

Every Stork command honors the `--help` option, which will cause it to
display usage information. Run, e.g., `stork submit --help` to see
detailed information on how to use the submit command.

Talking to Stork
================

Stork accepts job descriptors in a number of simple key-value pair
formats, including JSON and (simple) Condor ClassAd.

An example job descriptor in the JSON format:

    {
      "src"  : "ftp://example.com/file1.txt",
      "dest" : "ftp://example.com/file2.txt",
      "max_attempts" : 5,
      "email" = "user@example.com"
    }

The same job descriptor in the ClassAd format:

    [
      src  = "ftp://example.com/file1.txt";
      dest = "ftp://example.com/file2.txt";
      max_attempts = 5;
      email = "user@example.com"
    ]

When reading job descriptors from a file using `stork submit` the
opening and closing brackets may be omitted.

Stork is very liberal with what formats it accepts, and can receive
jobs in various formats with similar grammars to JSON/ClassAd, though
with weird combinations of symbols. How weird exactly? Weird enough
that the same parser understands both JSON and ClassAd. The parser can
be found in `stork.ad.AdParser` if you're curious exactly what you can
throw at this thing.

More information about JSON and Condor ClassAd can be found here:

  <http://www.json.org/>
  <http://research.cs.wisc.edu/condor/classad/>

Project Structure
=================

bin/
----
Contains scripts to execute JARs. This directory gets included in
the release tarfile for a binary release.

build/
------
Gets created when the project is built. Contains all .class files
generated by the Java compiler. Everything in here then gets put
into stork.jar after building.

lib/
----
Contains external libraries that get included in stork.jar on build.

libexec/
--------
Stork searches here for transfer module binaries when it is run.
Gets included in the binary release tarfile.

stork/
------
Includes all the .java files for Stork.

Makefile
--------
Contains all the build rules for make. You can manually configure
some options for Stork here.

README.md
---------
Contains instructions on how to use Stork. Gets included in the
binary release tarfile.
