# RedisQueue
Redis Queue of PHP


# Demo
## Add task
```php
<?php

        // queue config
        $queueName = 'myqueue';
        $redisConfig = [
            'host' => '127.0.0.1',
            'port' => '6379',
            'index' => '0'
        ];

        // add data
        $data = [
            'k1' => 'v1',
            'k2' => 'v2',
        ];

        echo "Queue name: " . $queueName . "\n";

        try {
            // create queue
            $redisQueue = new RedisQueue($queueName, $redisConfig);

            while(True){
                // add to queue
                $index = $redisQueue->add($data);

                echo "Data index: " . $index . "\n";  // index of added data
                sleep(1);
            }


        } catch (RedisQueueException $e) {
            echo $e->getMessage();
        }
```


## Handle task
```php
<?php
        // queue config
        $queueName = 'myqueue';
        $redisConfig = [
            'host' => '127.0.0.1',
            'port' => '6379',
            'index' => '0'
        ];

        echo "Queue name: " . $queueName . "\n";

        while (True){
            try {
                // create queue
                $redisQueue = new RedisQueue($queueName, $redisConfig);

                // fetch from queue
                $data = $redisQueue->get();
                echo "Fetched data:\n";
                //var_dump($data);

                // get the index of current data
                echo "Current Index:";
                $currentIndex = $redisQueue->getCurrentIndex($data);
                var_dump($currentIndex);
                echo $currentIndex;


                /* ... */
                $success = True;  //  process result
                //$success = False;  //  process result
                /* ... */

                sleep(2);

                if ($success) {  // success
                    $ret = $redisQueue->remove($data);
                    if (!empty($ret)) {
                        echo "\nData removed !";
                    }
                } else { // failed
                    echo "\nRollback current data";
                    $redisQueue->rollback($data);  // if retry times up to max, the index will be transfer to blocked list
                }
            } catch (RedisQueueException $e) {
                echo $e->getMessage();

            }
        }
```

## Restore blocked
```php
<?php
      $redisQueue = new RedisQueue($queueName, $redisConfig);
      $num = $redisQueue->repair();
```


## Show queue status
```php
<?php
        // queue config
        $queueName = 'myqueue';
        $redisConfig = [
            'host' => '127.0.0.1',
            'port' => '6379',
            'index' => '2'
        ];

        $redisQueue = new RedisQueue($queueName, $redisConfig);
        $ret = $redisQueue->status();
        var_dump($ret);
```


# Other Params
* **$queueName** 

* **$redisConfig** 
  Redis connect config.

* **$retryTimes**
  If the task handle failed,and up to retryLimit ,the task will be transfer to block task.

* **$waitTime**
  If there is no more task to handle, the 'get' action will be block.


# Data Flow
![image](https://github.com/woojean/RedisQueue/blob/master/imgs/redis-queue.png)



