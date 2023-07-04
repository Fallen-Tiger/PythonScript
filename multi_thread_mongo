import pymongo
import time
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from bson import ObjectId
from pymongo import UpdateMany


def multi_thread_find(query: dict) :
    cursor = test.find(query)
    res = []
    for r in cursor:
        res.append(r)
    return res


def count_num(collection, begin_id: int, end_id: int):
    num = collection.count_documents({"_id": {"$gte": ObjectId(str(hex(begin_id))[2:]),
                                                    "$lt": ObjectId(str(hex(end_id))[2:])}})
    # print("num:", num)
    return num


# 通过时间戳分片分成大致均匀的2份
def split_2(collection, begin_id: int, end_id: int) -> int:
    half = count_num(collection, begin_id, end_id) // 2
    left = begin_id
    right = end_id
    while left < right:
        mid_id = left + (right - left) // 2
        num_left = count_num(collection, begin_id, mid_id)
        # print("left:",str(hex(left))[2:],"right:",str(hex(right))[2:],"mid:",str(hex(mid_id))[2:])
        if num_left > half * 1.05:
            right = mid_id
        elif num_left < half * 0.95:
            left = mid_id
        else:
            return mid_id


# 返回16进制字符串格式
def split_n(collection, n) -> list:
    # 升序排列，取得最低_id
    begin = collection.find({}, {"_id": 1}).sort([("_id", 1)]).limit(1)[0]['_id']

    # 降序排列，取得最高的_id
    end = collection.find({}, {"_id": 1}).sort([("_id", -1)]).limit(1)[0]['_id']

    begin_id = int(begin.__str__(), 16)
    end_id = int(end.__str__(), 16)

    id_list = [begin_id, end_id]
    temp = n
    while temp != 1:

        ans_list = id_list
        # print(ans_list)
        for i in range(len(id_list) - 1):
            mid_id = split_2(collection, id_list[i], id_list[i + 1])
            ans_list.append(mid_id)
        ans_list.sort()
        id_list = ans_list
        temp /= 2
    return ans_list

# 用于多线程访问mongo数据库，将mongodb按照_id划分成2的幂次方份如32，开启32个线程分别进行遍历，2000w数据遍历约5分钟
# 单线程遍历约
if __name__ == '__main__':
    begin_time = time.time()
    test = connect_mongo("test")
    split_list = split_n(test, 32)
    # print(split_list)
    pool = ThreadPoolExecutor(32)
    res = []
    query = {}
    for i in range(32):
        id_query = {"_id": {"$gte": ObjectId(hex(split_list[i]).__str__()[2:]),"$lte": ObjectId(hex(split_list[i + 1]).__str__()[2:])}}
        t = pool.submit(multi_thread_find, id_query)
        res.append(t)
    sum = 0
    with open("data.txt",'w+') as f:
        for future in as_completed(res):
            data = future.result()
            for d in data:
                f.write(d)
            sum += len(data)
    print(sum)
    end_time = time.time()
    print("cost time: ", end_time-begin_time)
