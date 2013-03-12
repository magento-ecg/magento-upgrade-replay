Magento-Upgrade-Replay
======================

This is prototype for multi-process magento upgrade from Magento 1.5 (1.11 EE) and lower to 1.6 (1.11 EE) and higher.
Uses pdo_mysql.log to read queries and estimate execution time.

Disk may become bottleneck during upgrade replay.

#### Real life results
* 1.9 EE -> 1.11 EE ==> Original upgrade ~34 hours vs Optimized Replay 4 hours
* 1.4 CE -> 1.12 EE ==> Original upgrade ~15 hours vs Optimized Replay 2 hours

#### Requrements
* PHP 5.2
* POSIX extension
* Magento 1.6+ (1.11 EE) to upgrade to

#### How to use
1. Change few lines Varien_Db_Adapter_Pdo_Mysql
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

