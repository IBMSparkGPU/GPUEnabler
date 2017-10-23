Mavenized JCuda
=================

#### Features:

* Currently support **JCuda 0.5.0, 0.5.5, 0.6.0RC** (windows-i386, windows-x86_64), **0.6.0, 0.6.5** (windows-i386, windows-x86_64, unix-x86_64, mac-x86_64), **0.7.0a** (windows-x86_64, unix-x86_64, mac-x86_64, ppc64, ppc64le), **0.7.5** (windows-x86_64, unix-x86_64, mac-x86_64), **0.7.5b** (windows-x86_64)
* Local Maven repo with different sets of native libs (_windows-i386, windows-x86_64, unix-i386, unix-x86_64, mac-x86_64, ppc64, ppc64le_)
* Auto detection of OS family name and it's architecture (it's choose needed dependencies automatically)
* Running any main class, that contained JCuda code easily (without classpath hell, just run Maven goal)

#### How to run JCuda code:

* You need to install Cuda (5.0, 5.5, 6.0, 6.5, 7.0, 7.5) for your platform [here] [1]
* Set property **<jcuda.version>** in pom.xml to what you really use, e.g. 0.5.0 for Cuda 5.0, etc. (Cuda 6.5 - 0.6.5)
    * It looks like right now only **0.7.0a or higher** versions are usable due to adding of new library in 0.7.0a **JCuda**
    
    **OBSOLETE** [at least for now]
    
    * If you run **JCuda 0.6.0RC** and higher on **Windows** platform everything is ok
    * If you run **JCuda 0.5.5** and lower on **Windows** or **Unix** platform - you should set profile manually by adding *-P windows-x86_64_old*
        or *-P windows-x86_old* or *-P unix-x86_64_old*, etc. to _mvn clean package_ command, depends on architecture you use.
        For more info about **Maven** profiles take a look [here] [2]
    * If you run **JCuda 0.6.0** and higher on **Unix x86_64** platform - everything is ok
    * If you run **JCUda 0.6.0** and higher on **Mac x86_64** - everything is ok
    
    **OBSOLETE** [end of obsolete part]
       
    
* Call _mvn clean package_ to build project (it will copy all dependencies to _target/lib_ dir)
	* Users who configure a repository manager like Nexus should add option _-Dmaven.repo.local=repo_ to the _mvn clean package_ command.
* Call _mvn exec:exec_ to run main class (org.mystic.cuda.JCudaRuntimeTest) with "Hello, JCuda" sample :)
	* Users who configure a repository manager like Nexus should add option _-Dmaven.repo.local=repo_ to the _mvn exec:exec_ command.
* If you want to run code directly from your IDE without **Maven** - you could do it via _Run_ command in most of the IDE (Intellij IDEA, Eclipse, Netbeans, etc.) All you need to do - is to add property _-Djava.library.path=target/lib_ (more information is on [Stackoverflow] [3])
* ???
* Fork! Write your own JCuda code! Run! Report bugs! Support!

[1]: https://developer.nvidia.com/cuda-downloads "here"
[2]: http://maven.apache.org/guides/introduction/introduction-to-profiles.html "here"
[3]: http://stackoverflow.com/q/28333226/2663985
