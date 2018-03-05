<?php
/**
 * Created by PhpStorm.
 * User: woojean
 * Date: 2018/2/28
 * Time: 10:04
 */

namespace Look\Queue\RedisQueue;

/**
 * @property \Redis redis
 */
class RedisQueue
{
    const PREFIX = 'redisqueue';
    const ERROR_QUEUE_NAME_EMPTY = 'Queue name can not be empty!';
    const PROCESSING_INDEX = 'processing_index';

    public $queueName = '';

    public $redis = null;
    public $retryTimes = 3;
    public $waitTime = 3;


    /**
     * RedisQueue constructor.
     * @param $queueName
     * @param $redisConfig
     * [
     *   'host' => '127.0.0.1',
     *   'port' => '6379',
     *   'index' => 0,
     * ]
     * @param $retryTimes
     * @param $waitTime
     * @throws RedisQueueException
     */

    public function __construct($queueName, $redisConfig, $retryTimes = 3, $waitTime = 3)
    {
        if (empty($queueName)) {
            throw new RedisQueueException('getCurrentIndex:' . self::ERROR_QUEUE_NAME_EMPTY);
        }

        if (empty($redisConfig)) {
            throw new RedisQueueException('Redis config name can not be empty!');
        }

        $this->queueName = $queueName;
        $this->retryTimes = $retryTimes;
        $this->waitTime = $waitTime;

        $ret = $this->init($redisConfig);
        if (false === $ret) {
            throw new RedisQueueException('Queue init failed!');
        }
    }

    public function getIndexListName()
    {
        return self::PREFIX . ':index_list:' . $this->queueName;
    }

    public function getBlockedListName()
    {
        return self::PREFIX . ':blocked_list:' . $this->queueName;
    }

    public function getDataHashName()
    {
        return self::PREFIX . ':data_hash:' . $this->queueName;
    }

    public function getBlockedTimesHashName()
    {
        return self::PREFIX . ':blocked_times_hash' . $this->queueName;
    }


    public function getProcessingIndexName()
    {
        $keyName = self::PREFIX . ':' . self::PROCESSING_INDEX . ':' . $this->queueName;
        return $keyName;
    }

    public function getProcessingIndex()
    {
        $keyName = $this->getProcessingIndexName();
        return $this->redis->get($keyName);
    }

    public function setProcessingIndex($index)
    {
        $keyName = $this->getProcessingIndexName();
        return $this->redis->set($keyName, $index);
    }

    public function removeProcessingIndex()
    {
        $keyName = $this->getProcessingIndexName();
        return $this->redis->del($keyName);
    }


    public function init($redisConfig)
    {
        $this->redis = new \Redis();
        $this->redis->connect($redisConfig['host'], $redisConfig['port']);
        if (!empty($redisConfig['auth'])) {
            $this->redis->auth($redisConfig['auth']);
        }
        if (!empty($redisConfig['index'])) {
            $this->redis->select($redisConfig['index']);
        }
        return True;
    }


    public function genGuid()
    {
        return uniqid($this->queueName . '_');
    }

    public function addIndex($index)
    {
        $ret = $this->redis->lpush($this->getIndexListName(), $index);
        return $ret;
    }

    public function transferToBlocked($index)
    {
        $ret = $this->redis->lpush($this->getBlockedListName(), $index);
        return $ret;
    }


    public function getIndex()
    {
        $ret = $this->redis->rPop($this->getIndexListName());
        return $ret;
    }

    public function getData($index)
    {
        $ret = $this->redis->hGet($this->getDataHashName(), $index);
        $data = json_decode($ret, true);
        return $data;
    }


    public function addData($index, $data)
    {
        $data = json_encode($data, JSON_UNESCAPED_UNICODE);
        $ret = $this->redis->hSet($this->getDataHashName(), $index, $data);
        return $ret;
    }

    public function removeData($index)
    {
        $ret = $this->redis->hDel($this->getDataHashName(), $index);
        return $ret;
    }

    public function getBlockedTimes($processingIndex)
    {
        $ret = $this->redis->hGet($this->getBlockedTimesHashName(), $processingIndex);
        return intval($ret);
    }

    public function addBlocked($processingIndex)
    {
        $blockedTime = $this->redis->hGet($this->getBlockedTimesHashName(), $processingIndex);
        $blockedTime += 1;
        $ret = $this->redis->hSet($this->getBlockedTimesHashName(), $processingIndex, $blockedTime);
        return $ret;
    }

    public function getBlockedIndex()
    {
        $ret = $this->redis->rPop($this->getBlockedListName());
        return $ret;
    }

    public function removeBlockedTimes($index)
    {
        $ret = $this->redis->hDel($this->getBlockedTimesHashName(), $index);
        return $ret;
    }


    // ==== API ====================================================================

    // add new message
    public function add($data)
    {
        // make index
        $index = $this->genGuid();

        // add index
        $ret = $this->addIndex($index);
        if (false == $ret) {
            throw new RedisQueueException('Add index failed!');
        }

        // add data
        $ret = $this->addData($index, $data);
        if (false == $ret) {

            throw new RedisQueueException('Add data failed!');
        }

        return $index;

    }


    // get message
    public function get()
    {
        // check current key
        $processingIndex = $this->getProcessingIndex();
        if (!empty($processingIndex)) {
            throw new RedisQueueException('Have pending tasks!');
        }

        // get index
        $index = $this->getIndex();
        if (empty($index)) {
            sleep($this->waitTime);
            return null;
        }

        // get data
        $data = $this->getData($index);
        if (empty($data)) { // invalid index
            $this->remove();
        }

        // set current key
        $this->setProcessingIndex($index);

        return $data;
    }


    // remove message
    public function remove()
    {
        $processingIndex = $this->getProcessingIndex();

        // remove processing index
        $this->removeProcessingIndex();

        // remove data
        $ret = $this->removeData($processingIndex);
        return $ret;
    }


    // rollback message
    public function rollback()
    {
        $processingIndex = $this->getProcessingIndex();
        if ($processingIndex) {
            // add blocked times
            $this->addBlocked($processingIndex);

            // check blocked times
            $blockTimes = $this->getBlockedTimes($processingIndex);
            if ($blockTimes >= $this->retryTimes) { // if retry times up to max, the index will be transfer to blocked list
                // transfer to blocked list
                $ret = $this->transferToBlocked($processingIndex);
            } else {
                // rollback index
                $ret = $this->addIndex($processingIndex);
            }
            if (!empty($ret)) {
                // clear processing index
                $this->removeProcessingIndex();
            }
        }

    }

    // processing
    public function getCurrentIndex()
    {
        $processingIndex = $this->getProcessingIndex();
        return $processingIndex;
    }

    // repair
    public function repair()
    {
        $num = 0;
        // restore blocked
        $blockedIndex = $this->getBlockedIndex();
        while (!empty($blockedIndex)) {
            $this->addIndex($blockedIndex);

            // clear blocked times
            $this->removeBlockedTimes($blockedIndex);

            $blockedIndex = $this->getBlockedIndex();
            $num += 1;
        }

        // clear current index
        $this->removeProcessingIndex();

        return $num;
    }


    // status
    public function status()
    {
        $ret = [];
        $ret['total pending index'] = $this->redis->lLen($this->getIndexListName());
        $ret['total blocked index'] = $this->redis->lLen($this->getBlockedListName());
        return $ret;
    }

}