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
    const PREFIX = 'RQ';
    const ERROR_QUEUE_NAME_EMPTY = 'Queue name can not be empty!';
    const PROCESSING_INDEX = 'PI';
    const DATA_KEY = 'DK';

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
        // index list
        return self::PREFIX . ':IL:' . $this->queueName;
    }

    public function getBlockedListName()
    {
        // blocked list
        return self::PREFIX . ':BL:' . $this->queueName;
    }

    public function getDataHashName()
    {
        // data hash
        return self::PREFIX . ':DH:' . $this->queueName;
    }

    public function getBlockedTimesHashName()
    {
        // blocked times hash
        return self::PREFIX . ':BTH' . $this->queueName;
    }


    public function getDataKey($data)
    {
        return $data[self::DATA_KEY];
    }

    public function setDataKey($data, $index)
    {
        $data[self::DATA_KEY] = $index;
        return $data;
    }

    public function getProcessingIndexName($data)
    {
        $dataKey = $this->getDataKey($data);
        $keyName = self::PREFIX . ':' . self::PROCESSING_INDEX . ':' . $this->queueName . ':' . $dataKey;
        return $keyName;
    }

    public function getProcessingIndex($data)
    {
        $keyName = $this->getProcessingIndexName($data);
        return $this->redis->get($keyName);
    }

    public function setProcessingIndex($data)
    {
        $keyName = $this->getProcessingIndexName($data);
        $index = $this->getDataKey($data);
        return $this->redis->set($keyName, $index);
    }

    public function removeProcessingIndex($data)
    {
        $keyName = $this->getProcessingIndexName($data);
        return $this->redis->del($keyName);
    }

    public function removeAllProcessingIndex()
    {
        $pattern = self::PREFIX . ':' . self::PROCESSING_INDEX . ':' . $this->queueName . ':*';
        $keys = $this->redis->keys($pattern);
        foreach ($keys as $key) {
            $this->redis->del($key);
        }
    }


    public function init($redisConfig)
    {
        $this->redis = new \Redis();
        $this->redis->connect($redisConfig['host'], $redisConfig['port']);

        if(!empty($redisConfig['auth'])){
            $this->redis->auth($redisConfig['auth']);
        }

        if(!empty($redisConfig['index'])){
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

    public function removeData($data)
    {
        $index = $this->getDataKey($data);
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
        // get index
        $index = $this->getIndex();
        if (empty($index)) {
            sleep($this->waitTime);
            return null;
        }

        // get data
        $data = $this->getData($index);
        if (empty($data)) { // invalid index
            $data = $this->setDataKey($data, $index);
            $this->remove($index);
        }

        // set current key
        $data = $this->setDataKey($data, $index);
        $this->setProcessingIndex($data);

        return $data;
    }


    // remove message
    public function remove($data)
    {
        // remove processing index
        $this->removeProcessingIndex($data);

        // remove data
        $ret = $this->removeData($data);
        return $ret;
    }


    // rollback message
    public function rollback($data)
    {
        $processingIndex = $this->getProcessingIndex($data);
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
                $this->removeProcessingIndex($data);
            }
        }

    }

    // processing
    public function getCurrentIndex($data)
    {
        $processingIndex = $this->getProcessingIndex($data);
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
        $this->removeAllProcessingIndex();

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