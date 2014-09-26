<?php

include_once dirname(dirname(__FILE__)).'/app/Mage.php';
Mage::app();

class Upgrade_Replay {

    const CLEAN_BUFFER  = 'C';
    const READ_SQL      = 'S';
    const READ_SQL_LINE      = 'Q';
    const READ_BIND_LINE     = 'B';
    const DONE     = 'D';
    const SKIP_LINE     = 'S';

    protected $_queries = array();

    public function prepare($skipQuery = null)
    {

        $upgradeLog = 'pdo_mysql.log';
        $fp = fopen($upgradeLog,'rb');

        $matches = array();
        $sql = $bind = '';
        $state = self::CLEAN_BUFFER;
        $buffer = '';
        $totalTime = 0;

        $skip = !is_null($skipQuery);
        while ( ($line = fgets($fp)) !== false) {

            if ($skip && preg_match($skipQuery, $line)) {
                $skip = false;
                continue;
            }

            if ($skip) continue;



            if( preg_match('/^## [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/',$line)) {
                $state = self::CLEAN_BUFFER;
                $buffer = '';
                $sql = $bind = "";
            } elseif ( preg_match('/^## [0-9]+ ## QUERY/',$line) && $state == self::CLEAN_BUFFER) {
                //echo "next query: \r\n";
                $state = self::READ_SQL;
            } elseif ( preg_match('/^BIND: (.*)/',$line, $matches) && $state == self::READ_SQL_LINE) {
                //echo "I FOUND BIND!!!!\r\n";
                $sql = $buffer;
                $buffer = $matches[1];
                $state = self::READ_BIND_LINE;
            } elseif ( preg_match('/^AFF: (.*)/',$line, $matches) && (in_array($state, array(self::READ_SQL_LINE, self::READ_BIND_LINE)))) {
                if ($state == self::READ_BIND_LINE) {
                    $bind = $buffer;
                    //echo "SETTING BIND!!!\r\n";    exit;
                } else {
                    $sql = $buffer;
                }
                $buffer = '';
                $state = self::DONE;
            } elseif ( preg_match('/^TIME: ([0-9]+\.[0-9]+)/',$line,$matches) && ($state==self::DONE)) {
                $totalTime+=floatval($matches[1]);
                $this->_addQuery($sql, $bind, $matches[1]);

            } elseif ($state == self::READ_SQL && preg_match('/^SQL: (.*)/',$line, $matches)) {
                $match = $matches[1];
                if (!preg_match('/^(DESCRIBE |SHOW CREATE TABLE |SHOW TABLE STATUS LIKE |SELECT |SHOW INDEX FROM |SHOW TABLES)/', $match)
                ) {
                    //echo "Reading Line:\r\n$match";
                    $state =  self::READ_SQL_LINE;
                    $buffer .= $match;
                } else {
                    $state = self::SKIP_LINE;
                }
            } elseif (in_array($state, array(self::READ_SQL_LINE, self::READ_BIND_LINE))) {
                $buffer .= ' '.$line;
            } else {
                $time_match = array();
                if (preg_match('/^TIME: ([0-9]+\.[0-9]+)/',$line,$time_match)) {
                    $totalTime+=floatval($time_match[1]);
                }
                $state = self::SKIP_LINE;
            }

            if (!in_array($state, array(self::SKIP_LINE, self::CLEAN_BUFFER))) {
                //echo "$state > $line";
                //usleep(5000);
            }
        }
        echo "/*totalTime: ". $this->_getTimeString($totalTime) ."\r\n";
        $cleanupAffect = $this->_clean();
        var_dump($cleanupAffect);

        //$mergeIC =  $this->_mergeIndexAndConstraint();
        //var_dump($mergeIC);

        $merged = $this->_mergeQueries2();
        var_dump($merged);

        echo "*/";

        for ($i =1; $i <= 24;$i++) {
            $this->_estimate($i);
        }
        //$this->_estimate(4);
    }

    protected function _addQuery($sql, $bind, $time) {
        // 3 hour query fix
        if (preg_match('/INSERT INTO `salesrule_product_attribute`/', $sql)) {
            $sql = "
            drop temporary TABLE IF EXISTS tt1;
            create temporary table tt1
            as
            SELECT       sr.rule_id,
            --  cw.website_id,
            --     cg.customer_group_id,
                  ea.attribute_id,
                  CONCAT(sr.conditions_serialized, sr.actions_serialized) LIKE
            CONCAT( '%s:32:\"salesrule/rule_condition_product\";s:9:\"attribute\";s:',
            LENGTH(ea.attribute_code),  ':\"', ea.attribute_code, '\"%') as res

            from `salesrule` AS sr
            inner join `eav_attribute` AS ea  ON ea.entity_type_id = 10 ;

            INSERT INTO `salesrule_product_attribute`
            select sr.rule_id,
                cw.website_id,
                cg.customer_group_id,
            tmp.attribute_id
            from `salesrule` AS sr
            inner join (select * from tt1 where res = 1) as tmp ON tmp.rule_id = sr.rule_id
            INNER JOIN `core_website` AS cw
             ON FIND_IN_SET(cw.website_id, sr.website_ids)       INNER JOIN `customer_group` AS cg
             ON FIND_IN_SET(cg.customer_group_id , sr.customer_group_ids);
             drop temporary TABLE IF EXISTS tt1;
            ";
            $bind ='';
        }

        $bind_data =null;
        if($bind !='') {
            eval('$bind_data = '.$bind.";");
            //$bind_data=serialize($bind_data);
        }
        $query = array('sql'=>$sql, 'bind' => $bind_data, 'time'=> floatval($time));

        $this->_queries[] = $query;
    }

    /**
     * Remove duplicate queries and KEEP FIRST query mode
     */
    protected function _clean()
    {
        $keys = array_keys($this->_queries);
        sort($keys);
        $affected = array('index' => 0,'fk' => 0);
        foreach($keys as $queryKey) {
            $matches = array();
            if (!isset($this->_queries[$queryKey])) {
                continue;
            }

            if (preg_match('/ALTER TABLE `([a-zA-Z_0-9]+)` ADD (UNIQUE|INDEX) `([a-zA-Z_0-9]+)`/',$this->_queries[$queryKey]['sql'],$matches)) {
                //echo "match $matches[2]\r\n";
                foreach($keys as $queryKey_f) {
                    if (!isset($this->_queries[$queryKey_f]) || $queryKey >= $queryKey_f) {
                        continue;
                    }
                    if (preg_match('/ALTER TABLE `'.$matches[1].'` DROP (INDEX|KEY) `'.$matches[3].'`/',$this->_queries[$queryKey_f]['sql'])) {
                        unset($this->_queries[$queryKey]);
                        unset($this->_queries[$queryKey_f]);
                        $affected['index'] +=2;
                        break;
                    }
                }
                continue;
            }

            if (preg_match('/ALTER TABLE `([a-zA-Z_0-9]+)` ADD CONSTRAINT `([a-zA-Z_0-9]+)`/',$this->_queries[$queryKey]['sql'],$matches)) {

                foreach($keys as $queryKey_f) {
                    if (!isset($this->_queries[$queryKey_f]) || $queryKey >= $queryKey_f) {
                        continue;
                    }

                    if (preg_match('/ALTER TABLE `'.$matches[1].'` DROP FOREIGN KEY `'.$matches[2].'`/',$this->_queries[$queryKey_f]['sql'])) {
                        //echo "match ADD FK $matches[1] $matches[2]\r\n";
                        unset($this->_queries[$queryKey]);
                        unset($this->_queries[$queryKey_f]);
                        $affected['fk'] +=2;
                        break;
                    }
                }
                continue;
            }


        }
        return $affected;
    }


    protected function _mergeIndexAndConstraint()
    {
        $keys = array_keys($this->_queries);
        rsort($keys);
        $affected = 0;
        foreach($keys as $queryKey) {
            $matches = array();
            if (!isset($this->_queries[$queryKey])) {
                continue;
            }

            if (preg_match('/ALTER TABLE `([a-zA-Z_0-9]+)` ADD CONSTRAINT `([a-zA-Z_0-9]+)` FOREIGN KEY \(`([a-zA-Z_0-9]+)`\)/',$this->_queries[$queryKey]['sql'],$matches)) {
                //echo "match $matches[2]\r\n";
                foreach($keys as $queryKey_f) {
                    if (!isset($this->_queries[$queryKey_f]) || $queryKey <= $queryKey_f) {
                        continue;
                    }
                    if (preg_match('/ALTER TABLE `'.$matches[1].'` ADD (INDEX|KEY|UNIQUE) `([a-zA-Z_0-9]+)` \(`'.$matches[3].'`\)/',$this->_queries[$queryKey_f]['sql'], $addMatch)) {
                        $this->_queries[$queryKey]['sql'] = str_replace('FOREIGN KEY', 'FOREIGN KEY `'.$addMatch[2].'`', $this->_queries[$queryKey]['sql']);
                        unset($this->_queries[$queryKey_f]);
                        $affected +=1;
                        break;
                    }
                }
                continue;
            }
        }
        return $affected;
    }

    protected function _getDDLBlocks()
    {
        //ksort($similarStatements);
        $ddlBlocks = array(array());

        foreach ($this->_queries as $qId => $query) {
            if(preg_match('/ALTER TABLE/', $query['sql'])) {
                $ddlBlocks[max(array_keys($ddlBlocks))][] = $qId;
            } else {
                $ddlBlocks[] = array();
            }
        }

        foreach ($ddlBlocks as $blockId =>&$block) {
            if (empty($block)) {
                unset($ddlBlocks[$blockId]);
            } else {
               usort($block,array($this,'_sortQueries'));
            }
        }

        return $ddlBlocks;
    }


    /**
     * Sort queries in ddl block
     *
     * @param int $qId1 QueryId
     * @param int $qId2 QueryId
     */
    protected function _sortQueries($qId1, $qId2)
    {
        preg_match('/ALTER TABLE `([^`]+)` (.*)/',$this->_queries[$qId1]['sql'] ,$q1Matches);
        preg_match('/ALTER TABLE `([^`]+)` (.*)/',$this->_queries[$qId2]['sql'] ,$q2Matches);


        $ranks = array(
            '/\s*(DROP FOREIGN KEY)(.*)/',
            '/\s*(DROP INDEX|DROP KEY)(.*)/',
            '/\s*(DROP PRIMARY KEY)(.*)/',
            '/\s*(ADD COLUMN|MODIFY COLUMN)\s*`([^`]+)`(.*)/',
            '/\s*(CHANGE COLUMN|CHANGE)(.*)/',
            '/\s*(COMMENT|ENGINE|DEFAULT CHARACTER SET)(.*)/',
            '/\s*(ADD UNIQUE|ADD INDEX|ADD FULLTEXT|ADD PRIMARY KEY)(.*)/',
            '/\s*(ADD CONSTRAINT|DROP COLUMN)(.*)/',
            '/\s*(DISABLE KEYS|ENABLE KEYS)(.*)/',
        );
        $q1Rank =  $q2Rank =  -1;
        foreach ($ranks as  $rank => $regex) {
            if (preg_match($regex, $q1Matches[2]) && $q1Rank = -1) {$q1Rank = $rank;}
            if (preg_match($regex, $q2Matches[2]) && $q2Rank = -1) {$q2Rank = $rank;}
        }
        if ($q1Rank < 0 || $q2Rank <0) {
            echo "UNKNOWN ALTER";
            var_dump($q1Matches, $q2Matches);
            exit;
        }

        $res = 0;
        if ($q1Rank < $q2Rank) {
            $res = -1;
        } else if($q1Rank > $q2Rank) {
            $res = 1;
        } else if ($q1Matches[1] < $q2Matches[1]) {
            $res = -1;
        } else if ($q1Matches[1] > $q2Matches[1]) {
            $res = 1;
        } else if ($q1Matches[2] < $q2Matches[2]) {
            $res = -1;
        } else if ($q1Matches[2] < $q2Matches[2]) {
            $res = 1;
        }

        return $res;
    }

    /**
     * Merge LaterStatement for same tables
     */
    protected function _mergeQueries2()
    {
        $affected = 0;
        // MERGING Backward and forward
        $similarStatements = array(
            '/\s*(DROP FOREIGN KEY)(.*)/',
            '/\s*(DROP INDEX|DROP KEY)(.*)/',
            '/\s*(DROP PRIMARY KEY)(.*)/',
            '/\s*(ADD COLUMN|MODIFY COLUMN|COMMENT|ENGINE|DEFAULT CHARACTER SET)(.*)/',
            '/\s*(CHANGE COLUMN|CHANGE|COMMENT|ENGINE|DEFAULT CHARACTER SET)(.*)/',
            '/\s*(ADD UNIQUE|ADD INDEX|ADD FULLTEXT|ADD PRIMARY KEY)(.*)/',
            '/\s*(ADD CONSTRAINT|DROP COLUMN)(.*)/',
        );
        $ddlBlocks = $this->_getDDLBlocks();

        foreach ($ddlBlocks as $block) {
            foreach ($block as $q1 =>$qId1) {
                if(!isset($this->_queries[$qId1])) continue;
                foreach ($block as $q2 =>$qId2) {
                    //Move forward
                    if(!isset($this->_queries[$qId2]) || $q1>=$q2
                        || !empty($this->_queries[$qId1]['bind'])
                        || !empty($this->_queries[$qId2]['bind'])) continue;
                    $q1Matches = $q2Matches = array();
                    preg_match('/ALTER TABLE `([^`]+)`([^`]+)`([^`]+)`(.*)/',$this->_queries[$qId1]['sql'] ,$q1Matches);
                    preg_match('/ALTER TABLE `([^`]+)`([^`]+)`([^`]+)`(.*)/',$this->_queries[$qId2]['sql'] ,$q2Matches);
                    if (count($q1Matches) && $q1Matches[1] == $q2Matches[1] && $q1Matches[2] == $q2Matches[2] && $q1Matches[3] == $q2Matches[3]){
                        //echo "Similar $qId1,$qId2 \r\n";
                        //var_dump($q1Matches, $q2Matches);
                        unset($this->_queries[min($qId1,$qId2)]);
                        break;
                    }
                }
            }
        }

        foreach ($ddlBlocks as $block) {
            foreach ($block as $q1 =>$qId1) {
                if(!isset($this->_queries[$qId1])) continue;
                foreach ($block as $q2 =>$qId2) {
                    //Move forward
                    if(!isset($this->_queries[$qId2]) || $q1>=$q2
                        || !empty($this->_queries[$qId1]['bind'])
                        || !empty($this->_queries[$qId2]['bind'])) continue;
                    $q1Matches = $q2Matches = array();
                    preg_match('/ALTER TABLE `([^`]+)`(.*)/',$this->_queries[$qId1]['sql'] ,$q1Matches);
                    preg_match('/ALTER TABLE `([^`]+)`(.*)/',$this->_queries[$qId2]['sql'] ,$q2Matches);
                    // Stop merging if table is different
                    if ($q1Matches[1] != $q2Matches[1]) break;
                    // Check is statement can be merged;
                    $areSimilar = false;
                    foreach ($similarStatements as $regex) {
                        // Merge only if both are matching description
                        if (preg_match($regex,$q1Matches[2]) && preg_match($regex,$q2Matches[2])) {
                            $areSimilar = true;
                            break;
                        }
                    }
                    // if they are similar, we can merge them
                    if ($areSimilar) {
                        //$this->_queries[max($qId1,$qId2)]['sql'] .= $this->_queries[min($qId1,$qId2)]['sql'] . ', '. $q2Matches[2];
                        //unset($this->_queries[min($qId1,$qId2)]);

                        $this->_queries[$qId1]['sql'] .= ', '. $q2Matches[2];
                        unset($this->_queries[$qId2]);
                        $affected++;
                    } else {
                        //var_dump($this->_queries[$qId1]['sql'], $this->_queries[$qId2]['sql']);
                    }

                }

            }
            //exit;
        }
        return $affected;
    }

    public function echoQueries()
    {
        //return;
        $estimatedTime = 0;
        foreach ($this->_queries as $qid => $query)
        {

            echo "/* QID:$qid */ ". $query['sql'] .";\r\n";
            if ($query['bind']) {
                echo  '/* '. serialize($query['bind']) ." */\r\n";
            }
            $estimatedTime += $query['time'];
        }
        //echo '-- Estimated Time: '. $this->_getTimeString($estimatedTime);
    }

    protected function _estimate($threadNum)
    {
        $estimatedTime = 0;
        $empty_threads = array_pad(array(),$threadNum,0);

        /**
         * There will 2 Sync points:
         * and 2 point of going parallel.
         * Going parallel:
         * 1. ALTER TABLE WITH ADD CONSTRAINT
         * 2. ALTER TABLE WITH CONSTRAINT
         * Sync points:
         * 1. FIRST ALTER TABLE WITH ADD CONSTRAINT
         * 2. ALTER TABLE WITH CONSTRAINT
         */
        $acMode = false;

        $threads = $empty_threads;
        echo "\r\n/* ";
        foreach ($this->_queries as $query)
        {
            if ($query) {

            }

            $match = array();
            if (preg_match('/ALTER TABLE `([^`]+)`(.*)/', $query['sql'], $match)) {
                if (preg_match('/ADD CONSTRAINT/',$match[2]) && $acMode!=1) {
                    $acMode=1;
                    $estimatedTime+=max($threads);
                    $threads = $empty_threads;
                } elseif (preg_match('/(DROP INDEX|DROP KEY)/',$match[2]) && $acMode!=2) {
                    $acMode=2;
                    $estimatedTime+=max($threads);
                    $threads = $empty_threads;
                } elseif($acMode !=3) {
                    $acMode=3;
                    $estimatedTime+=max($threads);
                    $threads = $empty_threads;
                }
                $threads[0] += $query['time'];
                sort($threads);
            } else {
                $acMode = false;
                $oldEst = $estimatedTime;
                $estimatedTime +=max($threads) + $query['time'];
                //echo "\r\nDML sync: " . max($threads). ' AND '.$query['time'];
                $threads = $empty_threads;
            }
        }
        echo "\r\n ESTIMATED TIME AT $threadNum threads:". $this->_getTimeString($estimatedTime) ." */";
    }

    protected function _getNextQid()
    {
        foreach($this->_queries as $qId=>&$query) {
            if (isset($query['status']) && $query['status'] == 'finished') continue;
            $canStart = true;
            foreach($query['dep'] as $depId) {
                if(!isset($this->_queries[$depId]['status']) || $this->_queries[$depId]['status'] != 'finished') {
                    $canStart = false;
                    break;
                }
            }
            if ($canStart)  {
                return $qId;
            }
        }
        return null;
    }

    protected function _getParallelQueries()
    {
        echo "-- Preparing queries";
        $queryFlow = array(array());

        $estimatedTime = 0;
        //$empty_threads = array_pad(array(),$threadNum,0);

        /**
         * There will 2 Sync points:
         * and 2 point of going parallel.
         * Going parallel:
         * 1. ALTER TABLE WITH ADD CONSTRAINT
         * 2. ALTER TABLE WITH CONSTRAINT
         * Sync points:
         * 1. FIRST ALTER TABLE WITH ADD CONSTRAINT
         * 2. ALTER TABLE WITH CONSTRAINT
         */
        $acMode = 0;

        //$threads = $empty_threads;
        echo "\r\n/* ";
        foreach ($this->_queries as $qId=>$query)
        {
            $match = array();
            if (preg_match('/ALTER TABLE `([^`]+)`(.*)/', $query['sql'], $match)) {
                if (preg_match('/ADD CONSTRAINT/',$match[2]) && $acMode!=1) {
                    $acMode=1;
                    $queryFlow[] = array();
                } elseif (preg_match('/(DROP INDEX|DROP KEY)/',$match[2]) && $acMode!=2) {
                    $acMode=2;
                    $queryFlow[] = array();
                } elseif($acMode !=3) {
                    $acMode=3;
                    $queryFlow[] = array();
                }
                $queryFlow[max(array_keys($queryFlow))][] = $qId;
            } else {
                $queryFlow[] = array($qId);
                $acMode = false;
            }
        }
        echo "\r\n*/";
        return $queryFlow;
    }

    public function run($threads, $real = false)
    {
        $parallelQueries = $this->_getParallelQueries();
        $app = Mage::app();
        $app->cleanCache();
        echo "\nCleaned Cache\r\n";
        $queryIds = array_keys($this->_queries);
        sort($queryIds);
        /** @var $connection Zend_Db_Adapter_Abstract */
        $connection = Mage::getSingleton('core/resource')->getConnection('default_setup');
        $connection->closeConnection();
        // init callback pipe
        $waitPipe = dirname(__FILE__).'/upgrade_'.getmypid().'.pipe';
        $waitPipeMode = 0600;
        posix_mkfifo($waitPipe, $waitPipeMode);
        //touch($waitPipe);
        echo "Creating Pipe $waitPipe\r\n";


        $descriptorspec = array(
            0 => array("pipe", "r"),  // stdin
            1 => array("pipe", "w"),  // stdout
            2 => array("pipe", "w")   // stderr ?? instead of a file
        );
        $procs = $pipes = array();

        echo "\r\nopening workers";
        for ($i = 0; $i<$threads;$i++) {

            /* ACTUAL CODE OF WORKER */
             // NIGHTMARE Worker Program To run queries and send response in pipes
            $workerCode = "include_once '".dirname(dirname(__FILE__))."/app/Mage.php'; Mage::app(); ".
                "\$responsePipe=fopen('".$waitPipe."','ab');".
                "\$myWorkerId = ".$i."; ".
                "\$connection = Mage::getSingleton('core/resource')->getConnection('default_setup');".
                'function runQuery($sQuery) {global $connection, $responsePipe, $myWorkerId; '.
                '$query = unserialize($sQuery);'.
                ($real?'try{ $connection->query("SET SQL_MODE=\'\',@OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0,'.
                    ' @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE=\'NO_AUTO_VALUE_ON_ZERO\';".$query["sql"].";SET SQL_MODE=IFNULL(@OLD_SQL_MODE,\'\'), FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS=0, 0, 1); ",'.
                    '(empty($query["bind"])?array():$query["bind"])); $res = "OK";}catch(Exception $e) {echo $e; $res = "error";}'
                    :'$res = $connection->query("show processlist;")->fetchAll(); $res="OK"; ').
                ' $connection->closeConnection(); return $res; }'.
                "ini_set('track_errors',true); do{ ob_start(); \$_R_=array('','','');".
                "@\$_C_=fread(STDIN,array_pop(unpack('N',fread(STDIN,4)))); @trigger_error(''); ".
                "if (eval('return true;if(0){'.\$_C_.'}')) { try { \$_R_[0]=eval(\$_C_); }".
                " catch (Exception \$e) { \$php_errormsg = \$e; } } else { \$_R_[0]='E;'; } ".
                "\$_R_[1]=\$php_errormsg; \$_R_[2]=ob_get_clean(); \$_RS_ = serialize(\$_R_); ".
                "fwrite(STDOUT,pack('N',strlen(\$_RS_)).\$_RS_); fwrite(\$responsePipe, \$myWorkerId.\"\\r\\n\");} while(!empty(\$_C_)); exit;";
            //echo "\r\n$workerCode\r\n";
            $workerProc = "php -d memory_limit=2000M -r ". escapeshellarg($workerCode);
            //echo $workerProc."\r\n";
            $procs[$i]= proc_open($workerProc,$descriptorspec, $pipes[$i]);
            stream_set_blocking($pipes[$i][0],false);
            stream_set_blocking($pipes[$i][1],false);
            stream_set_blocking($pipes[$i][2],false);
        }

        $queryIds = array_keys($this->_queries);
        sort($queryIds);
        $queryInThread = array();
        $justCounter = 0;

        $finish = false;
        $errors = array();
        $error = '';
        $busyArray = array_pad(array(),$threads,0);
        // Sending Queries to each pipe
        foreach ($parallelQueries as $queryIds)
        {
            // Skipping Empty
            if ( empty($queryIds)) {
                 continue;
            }
            //send:
            foreach ($queryIds as $nextQueryId) {

                if ($this->_queries[$nextQueryId]['sql'] == "UPDATE `core_resource` SET `code` = ?, `data_version` = ? WHERE (code = 'core_setup')") {
                    echo " -- $justCounter/".(count($this->_queries)-1)."\r\n";
                    echo "Running Data Upgrade\r\n";
                    var_dump($connection->query("show processlist")->fetch());
                    if ($real) Mage_Core_Model_Resource_Setup::applyAllDataUpdates();
                    $finish =true;

                }

                if (min($busyArray)>0) {
                    // Open pipe if needed;
                    if (!isset($waitPipeResource)) {
                        echo "\r\nWaiting for replies from\r\n";
                        $waitPipeResource = fopen($waitPipe,"rb");
                    }
                    $workerId = (int)fgets($waitPipeResource, 1000);
                    //$workerId = (int)$workerId;
                    $output = unserialize(fread($pipes[$workerId][1],array_pop(unpack('N',fread($pipes[$workerId][1],4)))));
                    $error = fread($pipes[$workerId][2],100000);
                    //var_dump($output, $error);
                    $nextWorker = $workerId;

                    if (!empty($error)) {
                        $errors[]= array( $nextWorker, $output,$error);
                        break;
                    }
                    if ($output[0] != 'OK') {
                        var_dump($this->_queries[$queryInThread[$nextWorker]]);
                        echo $output[2]."\r\n";
                    }
                    $this->_queries[$queryInThread[$nextWorker]]['status']= 'finished';
                } elseif(count($queryIds) > 1) {
                    asort($busyArray);
                    reset($busyArray);
                    $nextWorker = key($busyArray);
                } else {
                    $nextWorker =0;
                }
                //echo "-- Sending $nextQueryId to $nextWorker\r\n";
                $this->_sendToThread($pipes[$nextWorker][0], $nextWorker, $nextQueryId);
                $queryInThread[$nextWorker] = $nextQueryId;

                $busyArray[$nextWorker] = 1;
                if($justCounter% 10==1) echo " -- $justCounter/".(count($this->_queries)-1)."";
                $justCounter++;
            }

            //sync;
            while (max($busyArray) > 0) {
                // Open pipe if needed;
                if (!isset($waitPipeResource)) {
                    echo "\r\nWaiting for replies, did not use all threads\r\n";
                    $waitPipeResource = fopen($waitPipe,"rb");
                }
                $workerId = (int)fgets($waitPipeResource, 1000);
                $output = unserialize(fread($pipes[$workerId][1],array_pop(unpack('N',fread($pipes[$workerId][1],4)))));
                $error = fread($pipes[$workerId][2],100000);
                if (!empty($error)) {
                    $errors[]= array($workerId, $output,$error);
                    echo "Emergency Shutdown!\r\n";
                }
                //var_dump($output, $error);
                //echo "loop\r\n";
                //var_dump($output, $error);
                $busyArray[$workerId] = 0;
                $this->_queries[$queryInThread[$workerId]]['status']= 'finished';
            }
            if (!empty($errors)) {
                echo "Emergency Shutdown!\r\n";
                echo "Dumping current query and state\r\n";
                var_dump($errors);
                foreach($queryInThread as $thread=> $qId) {
                    echo "Dumping thread $thread\r\n";
                    var_dump($thread, $this->_queries[$qId]);
                }
                break;
            }

            if ($output[0] != 'OK') {
                var_dump($this->_queries[$queryInThread[$nextWorker]]);
                echo $output[2]."\r\n";
            }

            if ($finish) {
                break;
            }
        }

        echo "closed Pipes\r\n";
        foreach($procs as $proc) {
            proc_close($proc);
        }
        echo "Closed Pipes\r\n";

        fclose($waitPipeResource);


        //getting reply in pipe, and send next query.

    }

    public function runSingle($real = false, $skipQuery = '')
    {
	    $skipFlag = !empty($skipQuery);
        echo "/* Starting\r\n";
        include_once dirname(dirname(__FILE__)).'/app/Mage.php';
        $app = Mage::app();
        $app->cleanCache();
        echo "\nCleaned Cache\r\n";
        $queryIds = array_keys($this->_queries);
        sort($queryIds);
        $connection = Mage::getSingleton('core/resource')->getConnection('default_setup');
        foreach ($queryIds as $counter=>$qid) {

    	    if ($skipFlag && preg_match($skipQuery,$this->_queries[$qid]['sql'])) {
    		$skipFlag = false;
    		continue;
    	    }
    	    if ($skipFlag) continue;
            if ($this->_queries[$qid]['sql'] == "UPDATE `core_resource` SET `code` = ?, `data_version` = ? WHERE (code = 'core_setup')") {
                echo "$counter/".(count($queryIds)-1)."\r\n";
                echo "Running Data Upgrade\r\n";
                if ($real) Mage_Core_Model_Resource_Setup::applyAllDataUpdates();
                break;
            }
            if ($real){
                try  {
                    if (!empty($this->_queries[$qid]['bind'])) {
                        $connection->query($this->_queries[$qid]['sql'],$this->_queries[$qid]['bind']);
                    } else {
                        $connection->query($this->_queries[$qid]['sql']);
                    }
                }catch (Exception $e) {
                    echo $e;
                    var_dump($this->_queries[$qid]['sql'], $this->_queries[$qid]['bind']);
                }
            }
            if($counter%10 == 7) echo "$counter/".(count($queryIds)-1)."\r\n";
        }

        echo "done */\r\n";

    }

    protected function _sendToThread($pipe, $thread, $qId)
    {
        $sQuery = " return runQuery('".str_replace("'","\\'",serialize($this->_queries[$qId]))."');";

        $this->_queries[$qId]['thread'] = $thread;
        $this->_queries[$qId]['status'] = 'started';
        fwrite($pipe,pack('N',strlen($sQuery)).$sQuery);
    }

    protected function _getTimeString($floatTime)
    {
        $hours = (int)($floatTime/3600);
        $minutes = (($floatTime/3600)-$hours)*60;

        return "$hours hours $minutes minutes";
    }

}
//var_dump('test:'. preg_match('/^## [0-9]+ ## QUERY/','## 9803 ## QUERY'));

$track = new Upgrade_Replay();
//$track->prepare("/ALTER TABLE `customer_entity` MODIFY COLUMN `website_id` smallint UNSIGNED NULL COMMENT ''/");
$track->prepare();

$track->echoQueries();
//$noOfThreads = 4;
echo date(DATE_RFC822) ." ---- Started\r\n";
//$track->runSingle(false);
//$track->runSingle(true);
//$track->run(4);
$track->run(12, true);

echo date(DATE_RFC822) ." ---- Finished\r\n";
//$track->runSingle(true,'/UNQ_CORE_CONFIG_DATA_SCOPE_SCOPE_ID_PATH/');
//$track->runSingle(true ,'/ALTER TABLE `core_config_data` ADD UNIQUE `UNQ_CORE_CONFIG_DATA_SCOPE_SCOPE_ID_PATH` (`scope`,`scope_id`,`path`)/');
//$track->runSingle(true,'ALTER TABLE `core_config_data` DROP KEY `config_scope`');








