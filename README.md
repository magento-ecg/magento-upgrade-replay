Magento-Upgrade-Replay
======================

This is prototype for multi-process Optimized Upgrade Replay from Magento 1.5 (1.10 EE) and lower to 1.6 (1.11 EE) and higher.
In order to run optimized replay you need to run original upgrade in database with same structure.
Replay script uses pdo_mysql.log to read queries and estimate execution time.

Disk may become bottleneck during upgrade replay.

#### Real life results
* 1.9 EE -> 1.11 EE ==> Original upgrade ~34 hours vs Optimized Replay 4 hours
* 1.4 CE -> 1.12 EE ==> Original upgrade ~15 hours vs Optimized Replay 2 hours

#### Requrements
* PHP 5.2+
* POSIX extension
* Magento 1.6+ (1.11 EE) to upgrade to

Prepare **Varien_Db_Adapter_Pdo_Mysql** in Magento 1.6+
Enable debug mode:
```php
    //protected $_debug               = false;  // Original
    protected $_debug               = true;
```
Log all queries:
```php
    // protected $_logAllQueries       = false;  // Original
   protected $_logAllQueries       = true;
```

#### Steps to upgrade
1. Trigger upgrade and wait until it ends.
2. Copy var/debug/pdo_mysql.log from upgraded Magento to shell direcory of new copy.
3. Copy upgrade_replay.php and shell directory of new copy.
4. cd [new copy]/shell && php -f upgrade_replay.php

#### Script internals

Create posix FIFO for inter-process communications.
Start multiple php processes using proc_open().
Prepare queries for execution:
* Strip out duplicate queries. 
* Merge similar ALTER TABLE queries.
* Optimize one data-specific query
* Split in chunks for parallel execution.
Child processes are waiting for data in STDIN and puting their IDs in fifo once process is finished.
Parent process reads fifo in blocking mode. 
Once Parent gets ID of child, it sends next query or waits till othe queries from chunk will be executed. 
Data upgrades are triggered the same way as in original.

#### Some code
Debug function to make shure that parsing FSM works fine. 
```php
$track->echoQueries(); 
```
Triggering upgrade in multiple threads ('true' will really trigger upgrade).
```php
// public function run($threads, $real = false)    
//$track->run(4);
$track->run(12, true);
```



