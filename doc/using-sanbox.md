---
title: Stratio Deep sandbox and demo
---

Table of Contents
=================

-   [Vagrant Setup](#vagrant-setup)
-   [Running the sandbox](#running-the-sandbox)
-   [What you will find in the sandbox](#what-you-will-find-in-the-sandbox)
-   [Access to the sandbox and other useful commands](#access-to-the-sandbox-and-other-useful-commands)
-   [Starting the Stratio Deep Shell and other useful commands](#starting-the-stratio-deep-shell-and-other-useful-commands)
-   [F.A.Q about the sandbox](#faq-about-the-sandbox)


Vagrant Setup
=============

To get an operating virtual machine with Stratio Deep distribution up and running, we use [Vagrant](https://www.vagrantup.com/).

-    Download and install [Vagrant](https://www.vagrantup.com/downloads.html). 
-    Download and install [VirtualBox](https://www.virtualbox.org/wiki/Downloads). 
-    If you are in a windows machine, we will install [Cygwin](https://cygwin.com/install.html).

Running the sandbox
===================

Initialize the current directory from the command line **`vagrant init stratio/deep-spark`**.

To facilitate the reading of the document , we will refer to this directory as /install-folder.

Please, be patient the first time it runs.

![Vagrant console initialization](images/vagrant-shell.png)

What you will find in the sandbox
=================================

-    OS: CentOS 6.5
-    3GB RAM - 2 CPU
-    Two ethernet interfaces.

Name|Version|Service name|Other
Spark | 1.1.1 | spark | service spark start
Stratio Deep| 0.6 | |service streaming start
Stratio Cassandra|2.1.05|cassandra|service cassandra start
Mongodb|2.6.5|mongod|service mongod start

Access to the sandbox and other useful commands
===============================================

Useful commands
---------------

-    Start the sandbox: **` vagrant up `**
-    Shut down the sandbox: **` vagrant halt `**
-    In the sandbox, to exit to the host: **` exit `**

Accessing the sandbox
---------------------
-    Located in /install-folder
-    **` vagrant ssh `**

Starting the Stratio Deep Shell
==============================================================

From the sandbox (vagrant ssh):

-    Starting the Stratio Deep Shell: **`/opt/sds/spark/bin/stratio-deep-shell`**
-    Exit the Stratio Stratio Deep Shell: **`exit`**

F.A.Q about the sandbox
=======================

##### **I am in the same directory that I copy the Vagrant file but I have this error:**

```
    A Vagrant environment or target machine is required to run this
    command. Run vagrant init to create a new Vagrant environment. Or,
    get an ID of a target machine from vagrant global-status to run
    this command on. A final option is to change to a directory with a
    Vagrantfile and to try again.
```

Make sure your file name is Vagrantfile instead of Vagrantfile.txt or VagrantFile.

______________________________________________________________________________________

##### **When I execute vagrant ssh I have this error:**

```
    ssh executable not found in any directories in the %PATH% variable. Is an
    SSH client installed? Try installing Cygwin, MinGW or Git, all of which
    contain an SSH client. Or use your favorite SSH client with the following
    authentication information shown below:
```

We need to install [Cygwin](https://cygwin.com/install.html) or [Git for Windows](http://git-scm.com/download/win).
